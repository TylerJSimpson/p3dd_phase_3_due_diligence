"""
nct_source_mapping

(1) Queries nct table in BigQuery to retrieve [source] and [nct_id] where [source] is unclean company name
(2) Writes results to parquet file and upload to GCS
(3) Queries nct_source_mapping table in BigQuery to compare previous dataframe to create a new dataframe where only new [nct_id] are grabbed
(4) WWrites the new dataframe with only new [nct_id] and corresponding [source]
"""

#import packages
from prefect import Flow, task
from google.cloud import bigquery
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from concurrent.futures import ThreadPoolExecutor
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import os

@task
def read_bq_nct():
    """
    Queries BigQuery table bronze.aact_studies 
    Returns companies in Phase 2/Phase 3 trials that have submitted an update within the last 3 months    
    Companies are part of industry and are either in testing or have compelted testing
    Writes data to pandas dataframe
    """
    
    # Instantiate the BigQuery client
    #client = bigquery.Client()
    client = bigquery.Client(project="dtc-de-0315")
    
    # Define the query
    query = """
        SELECT  source AS nct_source_name
        FROM    `dtc-de-0315.bronze.nct`
        WHERE   phase IN ("Phase 3", "Phase 2/Phase 3")
        AND     overall_status IN ("Active, not recruiting", "Completed")
        AND     source_class IN ("INDUSTRY")
        AND     last_update_submitted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        AND     source <> "[Redacted]"
        GROUP   BY source
    """
    
    # Execute the query
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()
    
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


@task
def compare_dataframes(data):
    """
    Reads data from BigQuery table bronze.nct_source_mapping and compares it with the new dataframe
    Returns only the rows from the new dataframe where the nct_id does not exist in the existing dataframe
    """
    client = bigquery.Client(project="dtc-de-0315")

    # Define the query to read existing data from BigQuery
    query = """
        SELECT nct_source_name
        FROM `dtc-de-0315.bronze.nct_source_mapping`
    """

    # Execute the query and convert the results to a pandas dataframe
    query_job = client.query(query)
    df_existing = query_job.to_dataframe()

    # Find the nct_id values in the new dataframe that are not present in the existing dataframe
    df_diff = data[~data.nct_source_name.isin(df_existing.nct_source_name)]

    # Add 'start_date' column to ensure new source company names have a start date for the SCD
    df_diff['start_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')


    return df_diff


@task()
def write_bq(df_diff: pd.DataFrame) -> None:
    """
    Write pandas dataframe to BiqQuery table bronze.nct_source_mapping
    Merges data with existing table based on nct_id column
    Appends new rows if nct_id does not exist
    """

    gcp_credentials_block = GcpCredentials.load("p3dd-gcp-credentials")

    df_diff.to_gbq(
        destination_table="bronze.nct_source_mapping",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@Flow
def nct_source_mapping():

    #Queries BigQuery and writes to pandas dataframe
    data = read_bq_nct()

    #Building parquet file
    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'nct_source_mapping_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

    #Upload parquet to GCS
    upload_to_gcs(parquet_file, 'p3dd-gcs-bucket', f'nct_source_mapping/bronze/{parquet_file}')

    #Delete local file
    delete_local_file(parquet_file)

    # Compare the new dataframe with the existing data in BigQuery
    df_diff = compare_dataframes(data)

    #Write data to BigQuery
    write_bq(df_diff)

if __name__ == '__main__':
    nct_source_mapping()