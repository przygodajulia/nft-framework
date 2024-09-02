from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import os

API_KEY = os.getenv('API_KEY')

# Test dag to check OpenSea API connection
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
    url = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=10&order_by=market_cap"
    headers = {
    "accept": "application/json",
    "x-api-key": API_KEY
    }   

    response = requests.get(url, headers=headers)
    return response.text 


# Function to save collections to S3
def save_collections_to_s3(**kwargs):
    ti = kwargs['ti']
    response_text = ti.xcom_pull(task_ids='retrieve_nft_collections')
    
    bucket_name = os.getenv('S3_BUCKET')
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
