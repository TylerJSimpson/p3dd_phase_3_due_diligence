"""
nct_postgres_to_gcs_and_bq

(1) Queries AACT Postgres database to gather National Clinical Trial (nct) data
(2) Creates Parquet file
(3) Writes Parquet file to GCS and BigQuery
"""


#Import packages
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


@task(retries=3)
def read_postgresql_credentials(config_file_path):
    """
    Reads postgresql credentials from local filepath on vm and assigns them to variables
    """
    config = ConfigParser()
    config.read(config_file_path)

    return {
        'host': config['postgresql']['host'],
        'port': config['postgresql']['port'],
        'database': config['postgresql']['database'],
        'username': config['postgresql']['username'],
        'password': config['postgresql']['password']
    }


@task(retries=3)
def query_postgresql(pg_credentials, query):
    """
    Queries postgresql AACT database main fact table [studies] and writes it to Pandas dataframe
    """
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


@task(retries=3)
def write_parquet_file(df, file_path, compression='snappy'):
    """
    Write pandas dataframe to parquet file
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path, compression=compression)


@task(retries=3)
def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    """
    Uploads parquet file to GCS
    """
    gcs_bucket = GcsBucket.load(bucket_name)
    gcs_bucket.upload_from_path(from_path=file_path, to_path=destination_blob_name)


@task(retries=3)
def delete_local_file(file_path):
    """
    Deletes local file on vm due to GCS being used as the primary storage
    """
    os.remove(file_path)


@task(retries=3)
def write_bq(data: pd.DataFrame) -> None:
    """
    Write pandas dataframe to BiqQuery table bronze.aact_studies
    Replaces (truncate + write) on each run
    """

    gcp_credentials_block = GcpCredentials.load("p3dd-gcp-credentials")

    data.to_gbq(
        destination_table="bronze.nct",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace", #truncate and write
    )


@Flow
def aact_postgres_to_gcs_and_bq():
 
    #Get Postgresql credentials from local config file
    pg_creds = read_postgresql_credentials('C:\\Users\simps\p3dd_phase_3_due_diligence\project\configuration\config.ini') #using this as relative path not working in deployment with Windows
    '''pg_creds = read_postgresql_credentials('../configuration/config.ini')'''

    #This query selects the entire [studies] table
    data = query_postgresql(pg_creds, "SELECT * FROM studies")

    #Building parquet file
    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'nct_studies_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)


    #Upload parquet to GCS
    upload_to_gcs(parquet_file, 'p3dd-gcs-bucket', f'nct/bronze/{parquet_file}')


    #Delete local file
    delete_local_file(parquet_file)

    #Write data to BigQuery
    write_bq(data)

if __name__ == "__main__":
    aact_postgres_to_gcs_and_bq()
