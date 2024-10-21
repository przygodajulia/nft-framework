from datetime import datetime, timedelta
import requests
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.airflow_util_scripts import get_secret_from_vault

API_KEY = get_secret_from_vault('api3', 'key')
BASE_URL = 'https://data-api.cryptocompare.com/index/cc/v1/historical/days?'
CRYPTO_SYMBOLS = ['USDC', 'POL', 'WETH', 'ETH', 'USDC.e']

START_DATE = 1719792000  # July 1, 2024 (in Unix time)
END_DATE = 1725081599    # September 30, 2024 (in Unix time)
LIMIT = 5000
S3_KEY_PREFIX = 'raw/exchange_rates_usd/'

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    dag_id='extract_crypto_exchange_rates',
    default_args=default_args,
    description='Fetch historical exchange rates for specified cryptocurrencies',
    schedule_interval=None,
    catchup=False,
)

def load_to_s3(data, symbol):
    """
    Load fetched exchange rate data to an S3 bucket.
    """
    try:
        s3_bucket = get_secret_from_vault("aws3", "s3bucket")
        s3_hook = S3Hook(aws_conn_id='aws_conn')

        s3_key = f"{S3_KEY_PREFIX}{symbol}/{symbol}_exchange_cadli.json"

        s3_hook.load_string(
            string_data=json.dumps(data),
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )

        logging.info(f"Successfully uploaded {symbol}_exchange.json to S3 bucket: {s3_bucket}")

    except Exception as e:
        logging.error(f"Failed to upload data to S3 for {symbol}: {e}")

def extract_exchange_rates():
    """
    Extract exchange rates from the CryptoCompare API.
    """
    for symbol in CRYPTO_SYMBOLS:
        logging.info(f"Starting for {symbol}")

        cursor_time = END_DATE
        all_data = []

        while True:
            params = {
                'market': 'cadli',
                'instrument': f'{symbol}-USD',
                'limit': LIMIT,
                'to_ts': cursor_time,
                'aggregate': 1,
                'fill': 'true',
                'apply_mapping': 'true',
                'response_format': 'JSON',
            }

            headers = {
                'Authorization': f'Apikey {API_KEY}',
            }

            try:
                response = requests.get(BASE_URL, headers=headers, params=params)
                logging.info(response.url)
                response.raise_for_status()
                data = response.json()

                if not data.get('Data'):
                    logging.info(f"No more data available for symbol: {symbol}")
                    break

                all_data.extend(data['Data'])

                min_timestamp = min(item['TIMESTAMP'] for item in data['Data'])
                logging.info(min_timestamp)

                if min_timestamp < START_DATE:
                    break

                cursor_time = min_timestamp

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data for {symbol}: {e}")
                break
        
        load_to_s3(all_data, symbol)


extract_rates_task = PythonOperator(
    task_id='extract_exchange_rates_task',
    python_callable=extract_exchange_rates,
    dag=dag,
)

extract_rates_task
