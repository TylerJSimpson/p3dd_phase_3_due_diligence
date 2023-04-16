"""
nct_source_mapping

(1) Queries nct table in BigQuery to retrieve [source] and [nct_id] where [source] is unclean company name
(2) Sends request to LinkedIn based on company name and returns number of jobs for each company
(3) Creates Parquet file
(4) Writes Parquet file to GCS and BigQuery
"""

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect import Flow, task
from google.cloud import bigquery

@task
def read_bq():
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
        SELECT  DISTINCT(source),
                nct_id
        FROM    `dtc-de-0315.bronze.nct`
        WHERE   phase IN ("Phase 3", "Phase 2/Phase 3")
        AND     overall_status IN ("Active, not recruiting", "Completed")
        AND     source_class IN ("INDUSTRY")
        --AND     last_update_submitted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        AND     source <> "[Redacted]"
        GROUP   BY source, nct_id
    """
    
    # Execute the query
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()
    
    return df