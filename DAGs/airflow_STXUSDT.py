from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import requests
from datetime import datetime
from google.cloud import storage
import google


symbol = "STXUSDT"
datasetid = "binancebatchdataset"
projectid = 'exemplary-proxy-415101'
bucket_name = "europe-west1-example-enviro-a86b81fd-bucket"
timestamp = datetime.now()
timestamp_string = str(timestamp.strftime('%Y-%m-%d_%H%M'))


credentials, _ = google.auth.default()
client = storage.Client(credentials=credentials)

# Save latest timestamp run
object_name = f"parameters/{symbol}/{symbol}_timestamp.state"
bucket = client.bucket(bucket_name)
blob = bucket.blob(object_name)
blob.upload_from_string(timestamp_string)


# Get latest file timestamp run
object_name = f"parameters/{symbol}/{symbol}_timestamp.state"
bucket = client.bucket(bucket_name)
blob = bucket.blob(object_name)
timestamp_run = blob.download_as_string()
timestamp_run = timestamp_run.decode("utf-8")


def convert_timestamp_to_string(timestamp):
    dt_object = datetime.fromtimestamp(timestamp / 1000.0)
    timestamp_str = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return timestamp_str

def get_historical_trades(symbol,limit):
    url = f"https://api.binance.com/api/v3/historicalTrades?symbol={symbol}&limit={limit}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
def my_python_function():

    historical_trades = get_historical_trades(symbol, 1000)

    # Open Latest ID state of Symbol.
    object_name = f"parameters/{symbol}/{symbol}.state"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    state = blob.download_as_string()
    state = int(state.decode("utf-8"))

    # Set response data into DataFrame
    df = pd.DataFrame(historical_trades)
    df = df[df['id']>state]

    lastest_id = str(df['id'].iloc[-1])

    # Update ID latest of Symbol
    blob.upload_from_string(str(lastest_id))



    # Upload Parquet File into gcStorage
    object_name = f"data/{symbol}/{symbol}_{timestamp_run}.parquet"
    blob = bucket.blob(object_name)
    parquet_buffer = df.to_parquet()
    blob.upload_from_string(parquet_buffer)
 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    f'load_parquet_to_bigquery_{symbol}',
    default_args=default_args,
    description='A DAG to load data from Parquet file to BigQuery',
    schedule_interval='*/5 * * * *',
)

python_task = PythonOperator(
    task_id='get_data_binance',
    python_callable=my_python_function,
    dag=dag,
)

# gs://europe-west1-example-enviro-a86b81fd-bucket/data/BTCUSDT/BTCUSDT_2024-02-24_0824.parquet

gcs_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_parquet_to_bigquery',
    bucket=bucket_name,
    source_objects=[f'data/{symbol}/{symbol}_{timestamp_run}.parquet'],
    destination_project_dataset_table=f'{projectid}.{datasetid}.{symbol}',
    source_format='parquet',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    dag=dag,
)

python_task >> gcs_to_bigquery_task
