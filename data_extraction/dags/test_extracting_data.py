from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Connection
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
import requests
import hvac

VAULT_ADDR = 'http://vault:8200'

# Helper functions for setting up connection
# TODO - store function in some other place
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

# Create AWS connection before the DAG starts
create_aws_connection()

# Test DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nft_collections_to_s3',
    default_args=default_args,
    description='A test DAG to retrieve NFT collections and save them to S3',
    schedule_interval=None,
    catchup=False
)

def retrieve_nft_collections(**kwargs):
    api_key = get_secret_from_vault('api1', 'key')
    url = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=10&order_by=market_cap"
    headers = {
        "accept": "application/json",
        "x-api-key": api_key
    }

    # TODO add some error control
    response = requests.get(url, headers=headers)
    return response.text

def save_collections_to_s3(**kwargs):
    ti = kwargs['ti']
    response_text = ti.xcom_pull(task_ids='retrieve_nft_collections')
    bucket_name = get_secret_from_vault('aws3', 's3bucket')
    s3_file_path = 'nft_collections_data.json'
    
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    with open('/tmp/nft_collections_data.json', 'w') as file:
        file.write(response_text)
    
    s3_hook.load_file(
        filename='/tmp/nft_collections_data.json',
        key=s3_file_path,
        bucket_name=bucket_name,
        replace=True
    )

retrieve_task = PythonOperator(
    task_id='retrieve_nft_collections',
    python_callable=retrieve_nft_collections,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_collections_to_s3',
    python_callable=save_collections_to_s3,
    provide_context=True,
    dag=dag,
)

retrieve_task >> save_task
