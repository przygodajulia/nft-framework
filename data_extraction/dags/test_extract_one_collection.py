from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Connection
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
import hvac
import requests
import json
# Set the path to include the airflow_utils directory
# Get the directory one level up
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow_utils')))
# print("Current sys.path:", sys.path)
# # Now import your functions
# from airflow_utilsairflow_helpers import create_aws_connection, get_secret_from_vault


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
        print("Root token file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_secret_from_vault(path, key):
    """
    Retrieves a secret from HashiCorp Vault at the specified path and key.

    This function connects to the Vault server using the root token, reads a secret
    from the given `path` and retrieves the value associated with the specified `key`.

    Example:
    For a secret specified in the following way: secret/aws1 keyid=your_secret_value
    mount_point: secret
    path: api1
    key: keyid

    Args:
        path (str): The path in the Vault from which to read the secret.
        key (str): The specific key within the secret to retrieve the value for.

    Returns:
        str: The value of the specified secret key if successful.
        None: If the secret cannot be read or the key is invalid.

    Raises:
        Exception: If Vault authentication fails.
        hvac.exceptions.InvalidRequest: If the request to Vault is invalid (e.g., path or key doesn't exist).
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
        print(f"Invalid request: {e}")
        return None
    
@provide_session
def create_aws_connection(session=None):
    """
    Creates an AWS connection in Airflow's metadata database if it doesn't already exist.

    This function checks for an existing AWS connection in Airflow's connection table
    with the connection ID `aws_conn`. If the connection does not exist, it creates a
    new connection using credentials retrieved from Vault and saves it to the database.

    The credentials are retrieved from Vault using the following paths:
    - 'aws1': Retrieves the AWS key ID.
    - 'aws2': Retrieves the AWS access key.

    Args:
        session (sqlalchemy.orm.session.Session, optional): SQLAlchemy session object automatically 
            provided by the `@provide_session` decorator. If not provided, the session is 
            created and managed by Airflow.

    Returns:
        None

    Raises:
        Exception: If there's an error retrieving the secrets from Vault.
    
    Notes:
        - This function uses the `get_secret_from_vault` function to fetch AWS credentials
          from Vault. Make sure Vault is accessible and configured properly.
        - The function prints messages indicating whether a new connection was created 
          or if it already existed.
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
        print(f"Created new connection: {conn_id}")
    else:
        print(f"Connection {conn_id} already exists.")

# create_aws_connection()

######################## Prepare DAG #########################

COLLECTION_SLUG = 'boredapeyachtclub'
BASE_URL = 'https://api.opensea.io/api/v2/events/collection/'
AFTER_TIMESTAMP = 1688169600  # July 1st, 2023
BEFORE_TIMESTAMP = 1696118399  # September 30th, 2023
S3_BUCKET = get_secret_from_vault('api1', 'key')
S3_KEY_PREFIX = '/opensea_data/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('opensea_collection_events_to_s3',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)

# Task to get events from OpenSea API
def get_opensea_events(cursor=None):

    api_key = get_secret_from_vault('api1', 'key')

    url = f"{BASE_URL}{COLLECTION_SLUG}"
    params = {
        'after': AFTER_TIMESTAMP,
        'before': BEFORE_TIMESTAMP,
        'event_type': ['sale', 'transfer', 'redemption'],
        'limit': 50
    }
    if cursor:
        params['next'] = cursor
    
    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key
    }

    response = requests.get(url, params=params, headers=headers)
    response_data = response.json()

    # Return events and cursor for the next page
    return response_data, response_data.get('next')

# Task to load data to S3
def load_to_s3(events, filename):
    s3_bucket = get_secret_from_vault("aws3", "s3bucket")
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_string(
        string_data=json.dumps(events), 
        key=f"{S3_KEY_PREFIX}{filename}.json", 
        bucket_name=s3_bucket, 
        replace=True
    )

# Task to extract data and store in S3
def extract_and_store():
    cursor = None
    page = 1
    while True:
        events, next_cursor = get_opensea_events(cursor)
        
        if not events:
            break

        load_to_s3(events, f'opensea_events_page_{page}')
        
        cursor = next_cursor
        page += 1

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_and_store_task',
    python_callable=extract_and_store,
    dag=dag
)

extract_task
