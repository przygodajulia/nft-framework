from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Connection
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
import hvac
import logging
import requests
import json
import os
from requests.exceptions import ConnectionError, Timeout

######################## Helper functions #########################

def read_root_token():
    """
    Reads the Vault root token from the shared-secrets volume.

    This function reads the `root-token.txt` file from the `/shared-secrets` directory
    and returns the root token as a string. If the file is not found or any other
    error occurs, it prints an appropriate error message.

    Returns:
        str: The root token read from the file, if successful. 
        None: If the file is not found or an exception occurs.

    Raises:
        FileNotFoundError: If the root-token.txt file does not exist.
        Exception: For any other exceptions during the file read operation.
    """
    try:
        with open('/shared-secrets/root-token.txt', 'r') as file:
            root_token = file.read().strip()
            return root_token
    except FileNotFoundError:
        logging.error("Root token file not found.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def get_secret_from_vault(path, key):
    """
    Retrieves a secret from HashiCorp Vault at the specified path and key.
    """
    client = hvac.Client(url='http://vault:8200', token=read_root_token())
    mount_point = 'secret'
    
    if not client.is_authenticated():
        raise Exception("Vault authentication failed. Please check your VAULT_TOKEN.")
    
    try:
        read_secret_result = client.secrets.kv.v1.read_secret(
            path=path,
            mount_point=mount_point
        )
        return read_secret_result['data'][key]
    except hvac.exceptions.InvalidRequest as e:
        logging.error(f"Invalid request: {e}")
        return None
    
@provide_session
def create_aws_connection(session=None):
    """
    Creates an AWS connection in Airflow's metadata database if it doesn't already exist.
    """
    conn_id = 'aws_conn'
    conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    
    if not conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type='aws',
            login=get_secret_from_vault('aws1', 'keyid'),
            password=get_secret_from_vault('aws2', 'accesskey')
        )
        session.add(new_conn)
        session.commit()
        logging.info(f"Created new connection: {conn_id}")
    else:
        logging.info(f"Connection {conn_id} already exists.")

######################## Prepare DAG #########################
COLLECTION_SLUGS = ['cryptopunks'] # Selected by market cap
BASE_URL = 'https://api.opensea.io/api/v2/events/collection/'
AFTER_TIMESTAMP = 1688169600  # July 1st, 2023
BEFORE_TIMESTAMP = 1696118399  # September 30th, 2023
S3_BUCKET = get_secret_from_vault('api1', 'key')
S3_KEY_PREFIX = 'raw/opensea_data/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('extract_nft_data_dag',
          default_args=default_args,
          schedule_interval=None,
          catchup=False)

def get_opensea_events(collection_slug, cursor=None):
    api_key = get_secret_from_vault('api1', 'key')

    logging.info(f"Extracting data for collection: {collection_slug}")

    url = f"{BASE_URL}{collection_slug}"
    params = {
        'after': AFTER_TIMESTAMP,
        'before': BEFORE_TIMESTAMP,
        'event_type': ['sale', 'transfer'],
        'limit': 50
    }
    if cursor:
        params['next'] = cursor
    
    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        response_data = response.json()

        if 'next' not in response_data or 'asset_events' not in response_data:
            logging.warning(f"Missing 'next' or 'asset_events' in response for collection: {collection_slug}")
            return None, None

        return response_data['asset_events'], response_data.get('next')
    
    except ConnectionError:
        logging.error(f"Connection error while trying to fetch data for {collection_slug}")
        return None, None
    except Timeout:
        logging.error(f"Request timed out for collection: {collection_slug}")
        return None, None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None, None

def load_to_s3(events, filename, collection_slug):
    """
    Load extracted events to an S3 bucket.
    """
    try:
        s3_bucket = get_secret_from_vault("aws3", "s3bucket")
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        
        s3_key = f"{S3_KEY_PREFIX}{collection_slug}/{filename}.json"
        
        s3_hook.load_string(
            string_data=json.dumps(events), 
            key=s3_key, 
            bucket_name=s3_bucket, 
            replace=True
        )
        logging.info(f"Successfully uploaded {filename} to S3 bucket: {s3_bucket}")
    
    except Exception as e:
        logging.error(f"Failed to upload file to S3: {e}")

def extract_and_store():
    """
    Extracts events from OpenSea API and stores them in S3.
    """
    for collection_slug in COLLECTION_SLUGS:
        cursor = None
        page = 1
        while True:
            events, next_cursor = get_opensea_events(collection_slug, cursor)
            
            if not events:
                logging.info(f"No more events found for collection: {collection_slug}")
                break

            load_to_s3(events, f'opensea_events_page_{page}', collection_slug)
            logging.info(f"Stored page {page} for collection: {collection_slug}")

            if not next_cursor:
                logging.info(f"No next cursor, finishing extraction for collection: {collection_slug}")
                break
            
            cursor = next_cursor
            page += 1

extract_task = PythonOperator(
    task_id='extract_and_store_task',
    python_callable=extract_and_store,
    dag=dag
)

extract_task