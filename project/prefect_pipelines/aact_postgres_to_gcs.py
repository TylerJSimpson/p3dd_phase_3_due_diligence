import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from configparser import ConfigParser
from prefect import task, Flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import datetime
import os

"""
TO-DO:
add logging
"""

@task
def read_postgresql_credentials(config_file_path):
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
def query_postgresql(pg_credentials, query):
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
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)


@task
def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    gcs_bucket = GcsBucket.load(bucket_name)
    gcs_bucket.upload_from_path(from_path=file_path, to_path=destination_blob_name)


@task
def delete_local_file(file_path):
    os.remove(file_path)


@Flow
def postgresql_to_gcs():
    pg_creds = read_postgresql_credentials('/home/tjsimpson/project/configuration/config.ini')

    data = query_postgresql(pg_creds, "SELECT * FROM studies LIMIT 10")

    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'aact_studies_test_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

    upload_to_gcs(parquet_file, 'zoom-gcs', f'project/bronze/{parquet_file}')

    delete_local_file(parquet_file)

if __name__ == "__main__":
    postgresql_to_gcs()
