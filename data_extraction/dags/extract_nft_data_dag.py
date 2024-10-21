from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Connection
from datetime import datetime, timedelta
import hvac
import logging
import requests
import json
import os
from requests.exceptions import ConnectionError, Timeout
from utils.airflow_util_scripts import get_secret_from_vault


COLLECTION_SLUGS = ['courtyard-nft'] # Selected by market cap pudgypenguins cryptopunks etc
BASE_URL = 'https://api.opensea.io/api/v2/events/collection/'
AFTER_TIMESTAMP = 1719792000  # July 1st, 2024
BEFORE_TIMESTAMP = 1725081599  # September 30th, 2024
S3_KEY_PREFIX = 'raw/opensea_nft_data/'

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