import psycopg2
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from configparser import ConfigParser
from prefect import task, Flow

# Initialize the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configure the logger to output to a file in GCS
storage_client = storage.Client.from_service_account_json('/path/to/credentials.json')
bucket = storage_client.bucket('my-bucket')
log_file = bucket.blob('logs/pipeline.log')
handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

@task
def read_postgresql_credentials(config_file_path):
    logger.info('Reading PostgreSQL credentials from config file')
    config = ConfigParser()
    config.read(config_file_path)

    return {
        'host': config['postgresql']['host'],
        'port': config['postgresql']['port'],
        'database': config['postgresql']['database'],
        'username': config['postgresql']['username'],
        'password': config['postgresql']['password']
    }

@task
def read_gcs_credentials(config_file_path):
    logger.info('Reading GCS credentials from config file')
    config = ConfigParser()
    config.read(config_file_path)

    return {
        'bucket': config['gcs']['bucket'],
        'credentials_path': config['gcs']['credentials_path']
    }

@task
def query_postgresql(pg_credentials, query):
    logger.info('Connecting to PostgreSQL database')
    conn = psycopg2.connect(
        host=pg_credentials['host'],
        port=pg_credentials['port'],
        database=pg_credentials['database'],
        user=pg_credentials['username'],
        password=pg_credentials['password']
    )

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df

@task
def write_parquet_file(df, file_path):
    logger.info('Writing data to Parquet file')
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

@task
def upload_to_gcs(file_path, gcs_credentials_path, bucket_name, gcs_object_name):
    logger.info('Uploading Parquet file to GCS')
    client = storage.Client.from_service_account_json(gcs_credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_object_name)
    blob.upload_from_filename(file_path)

# Define the flow for the pipeline
with Flow("postgresql-to-gcs") as flow:
    pg_creds = read_postgresql_credentials('/home/tjsimpson/project/configuration/config.ini')
    gcs_creds = read_gcs_credentials('/home/tjsimpson/project/configuration/config.ini')

    data = query_postgresql(pg_creds, "SELECT * FROM studies")

    parquet_file = 'data.parquet'
    write_parquet_file(data, parquet_file)

    upload_to_gcs(parquet_file, gcs_creds['credentials_path'], gcs_creds['bucket'], 'data.parquet')

# Run the pipeline
flow.run()
