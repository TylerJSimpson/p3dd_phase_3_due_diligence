"""
linkedin_to_gcs_and_bq

(1) Queries BigQuery
(2) Sends request to LinkedIn based on company name and returns number of jobs for each company
(3) Creates Parquet file
(4) Writes Parquet file to GCS and BigQuery
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
def read_bq():
    """
    Queries BigQuery table bronze.aact_studies 
    Returns companies in Phase 2/Phase 3 trials that have submitted an update within the last 3 months    
    Companies are part of industry and are either in testing or have compelted testing
    Writes data to pandas dataframe
    """
    
    # Instantiate the BigQuery client
    client = bigquery.Client()
    
    # Define the query
    query = """
        SELECT  linkedin_jobs_key,
                nct_id,
                source,
                CURRENT_TIMESTAMP() AS timestamp
        FROM    `dtc-de-0315.bronze.aact_studies`
        WHERE   phase IN ("Phase 3", "Phase 2/Phase 3")
        AND     overall_status IN ("Active, not recruiting", "Completed")
        AND     source_class IN ("INDUSTRY")
        AND     last_update_submitted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        AND     source <> "[Redacted]"
    """
    
    # Execute the query
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()
    
    return df

def get_job_openings(source):
    """
    Function to query US LinkedIn when given company name (source) and returns the number of current jobs
    """
    # Construct URL for LinkedIn search page
    search_url = f"https://www.linkedin.com/jobs/search/?keywords={source}&location=USA"

    # Send GET request asynchronously
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(requests.get, search_url)
        response = future.result()

    # Parse HTML response
    soup = BeautifulSoup(response.text, "html.parser")
    job_titles = []
    company_names = []

    for job in soup.find_all("h3", {"class": "base-search-card__title"}):
        job_title = job.text.strip()
        job_titles.append(job_title)

    for company in soup.find_all("h4", {"class": "base-search-card__subtitle"}):
        company_name = company.find("a").text.strip()
        company_names.append(company_name)

    job_dict = dict(zip(job_titles, company_names))

    return len(job_dict)

@task
def add_num_jobs(df):
    """
    For each company name (source) in pandas dataframe append the number of jobs from the function get_job_openings
    """
    df['num_jobs'] = df['source'].apply(get_job_openings)
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
    Write pandas dataframe to BiqQuery table bronze.linkedin_jobs
    Appends on each run
    """

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    data.to_gbq(
        destination_table="bronze.linkedin_jobs",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@Flow
def linkedin_to_gcs_and_bq():

    #Queries BigQuery and writes to pandas dataframe
    data = read_bq()
    
    #Parses LinkedIn passes company name (source) and gets the number of jobs (num_jobs) and adds to pandas dataframe
    data = add_num_jobs(data)

    #Building parquet file
    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'linkedin_jobs_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

    #Upload parquet to GCS
    upload_to_gcs(parquet_file, 'zoom-gcs', f'project/bronze/linkedin_jobs/{parquet_file}')

    #Delete local file
    delete_local_file(parquet_file)

    #Write data to BigQuery
    write_bq(data)

if __name__ == '__main__':
    linkedin_to_gcs_and_bq()
