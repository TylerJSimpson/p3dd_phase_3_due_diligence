"""
aact_postgres_to_gcs_and_bq

(1) Queries AACT Postgres database
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


@task
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


@task
def query_postgresql(pg_credentials, query):
    """
    Queries postgresql AACT database main fact table [studies] and writes it to pandas dataframe
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


@task
def write_parquet_file(df, file_path):
    """
    Write pandas dataframe to parquet file
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)


@task
def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    """
    Uploads parquet file to GCS
    """
    gcs_bucket = GcsBucket.load(bucket_name)
    gcs_bucket.upload_from_path(from_path=file_path, to_path=destination_blob_name)


@task
def delete_local_file(file_path):
    """
    Deletes local file on vm due to GCS being used as the primary storage
    """
    os.remove(file_path)


@task()
def write_bq(data: pd.DataFrame) -> None:
    """
    Write pandas dataframe to BiqQuery table bronze.aact_studies
    Replaces (truncate + write) on each run
    """

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
 
    #Get Postgresql credentials from local config file
    pg_creds = read_postgresql_credentials('/home/tjsimpson/project/configuration/config.ini')

    #This query selects the entire [studies] table and adds the key column linked_jobs_key by combining 'nct_id'_'source' and removing all spaces and special characters
    data = query_postgresql(pg_creds, "SELECT   *, REGEXP_REPLACE(CONCAT(nct_id, '_', source), '[^a-zA-Z0-9_]', '', 'g') AS linkedin_jobs_key FROM studies")

    #Building parquet file
    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'aact_studies_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

    #Upload parquet to GCS
    upload_to_gcs(parquet_file, 'zoom-gcs', f'project/bronze/aact_studies/{parquet_file}')

    #Delete local file
    delete_local_file(parquet_file)

    #Write data to BigQuery
    write_bq(data)

if __name__ == "__main__":
    aact_postgres_to_gcs_and_bq()