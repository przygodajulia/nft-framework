import hvac
import logging
from airflow.models import Connection
from airflow.utils.session import provide_session

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
