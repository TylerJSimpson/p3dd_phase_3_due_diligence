from prefect import task
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import Flow
import datetime

@task
def read_bq():
    """Reads a BigQuery table and returns a pandas DataFrame."""
    
    # Instantiate the BigQuery client
    client = bigquery.Client()
    
    # Define the query
    query = """
        SELECT  nct_id,
                source
        FROM    `dtc-de-0315.bronze.aact_studies`
        WHERE   phase IN ("Phase 3", "Phase 2/Phase 3")
        AND     overall_status IN ("Active, not recruiting", "Completed")
        AND     source_class IN ("INDUSTRY")
        AND     last_update_submitted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        ORDER   BY study_first_submitted_date DESC
    """
    
    # Execute the query
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()
    
    return df

@task
def write_parquet_file(df, file_path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

@Flow
def example_flow():
    data = read_bq()
    current_datetime = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
    parquet_file = f'aact_studies_test_{current_datetime}.parquet'
    write_parquet_file(data, parquet_file)

if __name__ == "__main__":
    example_flow()
