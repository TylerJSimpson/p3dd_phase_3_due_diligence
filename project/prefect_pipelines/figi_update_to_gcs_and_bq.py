from prefect import Flow, task
from google.cloud import bigquery
import requests
import configparser
import time
import pandas as pd
from datetime import datetime
import pyarrow as pa
from datetime import datetime
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import os
import pyarrow.parquet as pq

def map_common_stock_data(row):
    # Function to map common stock data from API responses to the row
    adr_data = row['adr_response_data']
    common_stock_data = row['common_stock_response_data']
    
    if adr_data == {'data': []}:
        if common_stock_data != {'data': []}:
            stock_data = common_stock_data['data'][0]
            # Map common stock data to row
            row['figi_id'] = stock_data['figi']
            row['figi_source_name'] = stock_data['name']
            row['ticker'] = stock_data['ticker']
            row['exchange_code'] = stock_data['exchCode']
            row['figi_composite'] = stock_data['compositeFIGI']
            row['security_type'] = stock_data['securityType']
            row['market_sector'] = stock_data['marketSector']
            row['share_class'] = stock_data['shareClassFIGI']
    else:
        if adr_data != {'data': []}:
            stock_data = adr_data['data'][0]
            # Map ADR data to row
            row['figi_id'] = stock_data['figi']
            row['figi_source_name'] = stock_data['name']
            row['ticker'] = stock_data['ticker']
            row['exchange_code'] = stock_data['exchCode']
            row['figi_composite'] = stock_data['compositeFIGI']
            row['security_type'] = stock_data['securityType']
            row['market_sector'] = stock_data['marketSector']
            row['share_class'] = stock_data['shareClassFIGI']
        elif common_stock_data != {'data': []}:
            stock_data = common_stock_data['data'][0]
            # Map common stock data to row
            row['figi_id'] = stock_data['figi']
            row['figi_source_name'] = stock_data['name']
            row['ticker'] = stock_data['ticker']
            row['exchange_code'] = stock_data['exchCode']
            row['figi_composite'] = stock_data['compositeFIGI']
            row['security_type'] = stock_data['securityType']
            row['market_sector'] = stock_data['marketSector']
            row['share_class'] = stock_data['shareClassFIGI']
    
    return row

@task
def read_bq_nct():
    # Task to query BigQuery table bronze.figi and read the results into a DataFrame
    client = bigquery.Client(project="dtc-de-0315")

    query = """
        SELECT  figi_primary_key,
                start_date,
                end_date,
                nct_source_name,
                linkedin_source_name
        FROM    `dtc-de-0315.bronze.figi`
        WHERE   linkedin_source_name IS NULL
        AND     end_date IS NULL
    """

    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()

    # Clean and process the data in the DataFrame
    df['nct_source_name_cleaned'] = (
        df['nct_source_name']
        .str.replace(',', '')
        .str.replace('.', '')
        .str.strip()
        .str.split()
        .str[:2]
        .apply(lambda x: ' '.join(x) if isinstance(x, list) else x)
    )

    return df

@task
def call_openfigi_api(query):
    # Task to call the OpenFIGI API with the provided query
    config = configparser.ConfigParser()
    
    config.read('C:\\Users\simps\p3dd_phase_3_due_diligence\project\configuration\config.ini') #using this as relative path not working in deployment with Windows
    #config.read('../configuration/config.ini')

    api_key = config.get('openfigi', 'X-OPENFIGI-APIKEY')

    url = 'https://api.openfigi.com/v3/search'
    headers = {
        'Content-Type': 'application/json',
        'X-OPENFIGI-APIKEY': api_key
    }

    adr_data = {
        "query": query,
        "exchCode": "US",
        "securityType": "ADR"
    }

    common_stock_data = {
        "query": query,
        "exchCode": "US",
        "securityType": "Common Stock"
    }

    # Make API requests for ADR and common stock data
    adr_response = requests.post(url, json=adr_data, headers=headers)
    common_stock_response = requests.post(url, json=common_stock_data, headers=headers)

    adr_response_data = adr_response.json()
    common_stock_response_data = common_stock_response.json()

    time.sleep(3)  # Delay to comply with API rate limits

    return query, adr_response_data, common_stock_response_data

@task
def process_api_responses(responses):
    # Task to process the API responses and create a DataFrame
    query, adr_response_data, common_stock_response_data = responses
    df = pd.DataFrame(
        {
            'nct_source_name_cleaned': [query],
            'adr_response_data': [adr_response_data],
            'common_stock_response_data': [common_stock_response_data]
        }
    )
    
    df = df.apply(map_common_stock_data, axis=1)  # Apply mapping function to each row
    
    return df

@task
def update_end_date(df):
    # Task to update the end_date column based on API response data
    def is_empty(response):
        return response == {'data': []} or response == {}

    def format_date(date):
        return datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d')  # Use hyphens instead of slashes

    # Update end_date based on conditions
    df['end_date'] = df.apply(lambda row: format_date(row['start_date']) if is_empty(row['adr_response_data']) and is_empty(row['common_stock_response_data']) else row['end_date'], axis=1)
    
    # Replace empty values with 'NULL' for end_date
    df['end_date'].replace('', 'NULL', inplace=True)
    
    return df

@task
def remove_columns(df):
    # Task to remove unnecessary columns from the DataFrame
    df = df.drop(['adr_response_data', 'common_stock_response_data', 'nct_source_name_cleaned'], axis=1)
    return df

@task
def write_dataframe_to_parquet(df, file_path, compression='snappy'):
    """
    Write pandas dataframe to parquet file
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path, compression=compression)

@task
def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    # Task to upload a file to GCS
    gcs_bucket = GcsBucket.load(bucket_name)
    gcs_bucket.upload_from_path(from_path=file_path, to_path=destination_blob_name)

@task
def delete_local_file(file_path):
    # Task to delete a local file
    os.remove(file_path)

@task
def delete_rows_from_bigquery():
    # Task to delete rows from BigQuery table
    client = bigquery.Client(project="dtc-de-0315")
    
    # Define the query
    query = """
        DELETE FROM `dtc-de-0315.bronze.figi` 
        WHERE figi_primary_key IN
        (
                SELECT figi_primary_key
                FROM `dtc-de-0315.bronze.figi`
                WHERE linkedin_source_name IS NULL
                AND end_date IS NULL
        )
    """
    # Execute the query
    client.query(query)

@task
def write_bq(combined_df: pd.DataFrame) -> None:
    # Task to write DataFrame to BigQuery
    gcp_credentials_block = GcpCredentials.load("p3dd-gcp-credentials")

    combined_df.to_gbq(
        destination_table="bronze.figi",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@Flow
def figi_update():
    # Main flow for the FIGI update process
    data = read_bq_nct()

    # First dataframe: BigQuery query results with nct_source_name_cleaned
    df1 = data.copy()

    # Second dataframe: API responses
    result_df = pd.DataFrame(columns=['nct_source_name_cleaned', 'adr_response_data', 'common_stock_response_data'])

    for query in data['nct_source_name_cleaned']:
        responses = call_openfigi_api(query)
        df = process_api_responses(responses)
        result_df = pd.concat([result_df, df], ignore_index=True)

        time.sleep(15)  # Delay between API requests

    # Merge the dataframes based on the matching values in 'nct_source_name_cleaned'
    combined_df = pd.merge(df1, result_df, on='nct_source_name_cleaned', how='inner', suffixes=('', '_api'))

    # Remove the duplicate columns from the right dataframe
    combined_df = combined_df.loc[:,~combined_df.columns.duplicated()]

    combined_df = update_end_date(combined_df)
    combined_df = remove_columns(combined_df)

    # Add the step to create the linkedin_source_name column
    combined_df['linkedin_source_name'] = '"' + combined_df['nct_source_name'] + '"'

    current_datetime = datetime.now().strftime('%m%d%Y_%H%M%S')
    filename = f'figi_update_{current_datetime}.parquet'

    write_dataframe_to_parquet(combined_df, filename)

    # Upload Parquet to GCS
    upload_to_gcs(filename, 'p3dd-gcs-bucket', f'figi/bronze/{filename}')

    # Delete local file
    delete_local_file(filename)

    delete_rows_from_bigquery()

    write_bq(combined_df)


if __name__ == '__main__':
    figi_update()
