"""

"""

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

@task()
def write_bq(data: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    data.to_gbq(
        destination_table="bronze.aact_studies",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace", #truncate and write
    )
'''
#This task will merge although to reduce the cost replace (truncate + write) is being used
@task()
def write_bq(data: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    client = bigquery.Client(project="dtc-de-0315", credentials=gcp_credentials_block.get_credentials_from_service_account())

    job_config = bigquery.QueryJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"  # Replace existing table
    job_config.destination = bigquery.TableReference.from_string("dtc-de-0315.bronze.aact_studies")

    query = f"""
    MERGE dtc-de-0315.bronze.aact_studies t
    USING (
        SELECT *
        FROM `{job_config.destination.project}.{job_config.destination.dataset_id}.{job_config.destination.table_id}`
        UNION ALL
        SELECT *
        FROM UNNEST({data.to_dict(orient="records")})
    ) s
    ON t.nct_id = s.nct_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """

    job = client.query(query, job_config=job_config)
    job.result()
'''

@Flow
def aact_postgres_to_gcs_and_bq():
    pg_creds = read_postgresql_credentials('/home/tjsimpson/project/configuration/config.ini')

    data = query_postgresql(pg_creds, "SELECT * FROM studies")

    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'aact_studies_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

    upload_to_gcs(parquet_file, 'zoom-gcs', f'project/bronze/{parquet_file}')

    delete_local_file(parquet_file)

    # Write Parquet file to BigQuery
    write_bq(data)

if __name__ == "__main__":
    aact_postgres_to_gcs_and_bq()
