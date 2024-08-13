from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import os

S3_BUCKET = os.getenv('S3_BUCKET')
S3_KEY = os.getenv('S3_KEY')

def create_empty_file_on_s3():
    """ Test function to check connections """
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_string('', S3_KEY, bucket_name=S3_BUCKET)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
}

with DAG(
    'test_airflow',
    default_args=default_args,
    description='Create an empty file on S3',
    schedule_interval=None,
    catchup=False

) as dag:
    
    create_file_task = PythonOperator(
    task_id='create_file_on_s3',
    python_callable=create_empty_file_on_s3,
    dag=dag,
    )

    create_file_task
    