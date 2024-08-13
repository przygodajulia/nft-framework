from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_spark_s3_job',
    default_args=default_args,
    description='Run Spark job to list files in S3 bucket and save results',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/bitnami/spark/jobs/test_spark_job.py',
        conn_id='spark_default',
        name='spark_submit',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client'
        },
        application_args=[
            os.getenv('S3_BUCKET'),
            os.getenv('S3_result_file')                   
        ],
        executor_memory='1G',
        total_executor_cores=1,
        dag=dag,
    )

    run_spark_job