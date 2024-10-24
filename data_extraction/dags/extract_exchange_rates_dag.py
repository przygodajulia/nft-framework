from datetime import datetime, timedelta
import requests
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.airflow_util_scripts import get_secret_from_vault

API_KEY = get_secret_from_vault('api3', 'key')
BASE_URL = 'https://min-api.cryptocompare.com/data/v2/histohour'
CRYPTO_SYMBOLS = ['USDC', 'POL', 'WETH', 'ETH', 'USDC.e']
START_DATE = 1719792000 # Load data for the second half of 2024
END_DATE = 1729775810 
LIMIT = 2000
S3_KEY_PREFIX = 'raw/exchange_rates_hourly_usdt/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='extract_crypto_exchange_rates',
    default_args=default_args,
    description='Fetch historical exchange rates for specified cryptocurrencies',
    schedule_interval=None,
    catchup=False,
)

def load_to_s3(data, symbol, file_num):
    """
    Load fetched exchange rate data to an S3 bucket.
    """
    try:
        s3_bucket = get_secret_from_vault("aws3", "s3bucket")
        s3_hook = S3Hook(aws_conn_id='aws_conn')

        s3_key = f"{S3_KEY_PREFIX}{symbol}/{symbol}_exchange_hourly_{file_num}.json"

        s3_hook.load_string(
            string_data=json.dumps(data),
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True,
        )

        logging.info(f"Successfully uploaded {symbol}_exchange_hourly.json to S3 bucket: {s3_bucket}")

    except Exception as e:
        logging.error(f"Failed to upload data to S3 for {symbol}: {e}")

def extract_exchange_rates():
    """
    Extract exchange rates from the CryptoCompare API.
    """
    for symbol in CRYPTO_SYMBOLS:
        logging.info(f"Starting extraction for {symbol}")

        cursor_time = END_DATE
        file_num = 0

        while True:

            params = {
                'fsym': symbol,
                'tsym': 'USD',
                'limit': 2000,
                'toTs': cursor_time,
                'e': 'CCCAGG', 
                'response_format': 'JSON',
            }

            headers = {
                'Authorization': f'Apikey {API_KEY}',
            }

            try:
                response = requests.get(BASE_URL, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                load_to_s3(data, symbol, file_num)
                file_num += 1

                data_key = data.get('Data')
                if not data_key:
                    logging.info(f"No more data available for symbol: {symbol}")
                    break

                cursor_time = min(item['time'] for item in data_key['Data'])
                logging.info(cursor_time)

                # Exit if the earliest timestamp is earlier than the START_DATE
                if cursor_time < START_DATE:
                    logging.info(f"Reached START_DATE for {symbol}. Stopping extraction.")
                    break

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data for {symbol}: {e}")
                break
            except KeyError as e:
                logging.error(f"KeyError encountered: {e}. Response data: {data}")  # Log the problematic data
                break


extract_rates_task = PythonOperator(
    task_id='extract_exchange_rates_task',
    python_callable=extract_exchange_rates,
    dag=dag,
)

extract_rates_task
