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
import pandas_gbq

def map_common_stock_data(row):
    adr_data = row['adr_response_data']
    common_stock_data = row['common_stock_response_data']
    
    if adr_data == {'data': []}:
        if common_stock_data != {'data': []}:
            stock_data = common_stock_data['data'][0]
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
    # Query BigQuery table bronze.figi
    client = bigquery.Client(project="dtc-de-0315")

    query = """
        SELECT  figi_primary_key,
                figi_id,
                figi_source_name,
                ticker,
                exchange_code,
                security_type,
                market_sector,
                figi_composite,
                share_class,
                start_date,
                end_date,
                nct_source_name,
                linkedin_source_name
        FROM    `dtc-de-0315.bronze.figi`
        WHERE   linkedin_source_name IS NULL
        AND     end_date IS NULL
        LIMIT   1
    """

    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()

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
    config = configparser.ConfigParser()
    config.read('../configuration/config.ini')

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

    adr_response = requests.post(url, json=adr_data, headers=headers)
    common_stock_response = requests.post(url, json=common_stock_data, headers=headers)

    adr_response_data = adr_response.json()
    common_stock_response_data = common_stock_response.json()

    time.sleep(3)

    return query, adr_response_data, common_stock_response_data


@task
def process_api_responses(responses):
    query, adr_response_data, common_stock_response_data = responses
    df = pd.DataFrame(
        {
            'nct_source_name_cleaned': [query],
            'adr_response_data': [adr_response_data],
            'common_stock_response_data': [common_stock_response_data]
        }
    )
    
    df = df.apply(map_common_stock_data, axis=1)
    
    return df


@task
def update_end_date(df):
    def is_empty(response):
        return response == {'data': []}

    def format_date(date):
        return datetime.strptime(str(date), '%Y-%m-%d').strftime('%m/%d/%Y')

    df['end_date'] = df.apply(lambda row: format_date(row['start_date']) if is_empty(row['adr_response_data']) and is_empty(row['common_stock_response_data']) else row['end_date'], axis=1)
    return df


@task
def write_dataframe_to_parquet(df, filename):
    df['linkedin_source_name'] = '"' + df['nct_source_name'] + '"'
    df.to_parquet(filename, index=False, compression='snappy', engine='pyarrow')

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
    Deletes local file on the VM due to GCS being used as the primary storage
    """
    os.remove(file_path)


"""
#potential skeleton to implement first deleting the columns and then writing them back
from prefect import task, Flow
from google.cloud import bigquery

@task
def delete_rows_from_bigquery():
    client = bigquery.Client()
    dataset_id = "your_dataset_id"
    table_id = "your_table_id"
    key_column = "your_key_column"
    key_value = "your_key_value"  # the specific value identifying the rows to delete
    
    # Construct the table reference
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Construct the SQL DELETE statement with a WHERE clause
    delete_query = f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE {key_column} = '{key_value}'"
    
    # Delete the rows
    client.query(delete_query).result()

# Define a Prefect flow
with Flow("Delete Rows from BigQuery Flow") as flow:
    delete_task = delete_rows_from_bigquery()

# Run the flow
flow.run()

#potential skeleton to then append the dataframe
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


"""

@task
def update_bq(df) -> None:
    """
    Update BigQuery table bronze.figi with the modified dataframe
    Updates fields based on figi_primary_key
    """

    gcp_credentials_block = GcpCredentials.load("p3dd-gcp-credentials")

    mapping = {
        "figi_primary_key": "figi_primary_key",
        "figi_id": "figi_id_api",
        "figi_source_name": "figi_source_name_api",
        "ticker": "ticker_api",
        "exchange_code": "exchange_code_api",
        "security_type": "security_type_api",
        "market_sector": "market_sector_api",
        "figi_composite": "figi_composite_api",
        "share_class": "share_class_api",
        "start_date": "start_date",
        "end_date": "end_date",
        "nct_source_name": "nct_source_name",
        "linkedin_source_name": "linkedin_source_name"
    }

    # Convert figi_primary_key to integer
    df['figi_primary_key'] = df['figi_primary_key'].astype(int)
 
    pandas_gbq.to_gbq(
        dataframe=df.rename(columns=mapping),  # assuming `mapping` is defined
        destination_table="bronze.figi_test",
        project_id="dtc-de-0315",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
        table_schema=[
            {"name": "figi_primary_key", "type": "INTEGER"},
            {"name": "figi_id", "type": "STRING"},
            {"name": "figi_source_name", "type": "STRING"},
            {"name": "ticker", "type": "STRING"},
            {"name": "exchange_code", "type": "STRING"},
            {"name": "security_type", "type": "STRING"},
            {"name": "market_sector", "type": "STRING"},
            {"name": "figi_composite", "type": "STRING"},
            {"name": "share_class", "type": "STRING"},
            {"name": "start_date", "type": "DATE"},
            {"name": "end_date", "type": "DATE"},
            {"name": "nct_source_name", "type": "STRING"},
            {"name": "linkedin_source_name", "type": "STRING"}
        ]
    )

@Flow
def figi_update():
    data = read_bq_nct()

    # First dataframe: BigQuery query results with nct_source_name_cleaned
    df1 = data.copy()

    # Second dataframe: API responses
    result_df = pd.DataFrame(columns=['nct_source_name_cleaned', 'adr_response_data', 'common_stock_response_data'])

    for query in data['nct_source_name_cleaned']:
        responses = call_openfigi_api(query)
        df = process_api_responses(responses)
        result_df = pd.concat([result_df, df], ignore_index=True)

        time.sleep(15)

    # Merge the dataframes based on the matching values in 'nct_source_name_cleaned'
    combined_df = pd.merge(df1, result_df, on='nct_source_name_cleaned', how='left', suffixes=('', '_api'))

    combined_df = update_end_date(combined_df)

    current_datetime = datetime.now().strftime('%m%d%Y_%H%M%S')
    filename = f'figi_update_{current_datetime}.parquet'

    write_dataframe_to_parquet(combined_df, filename)

    # Update BigQuery table
    update_bq(combined_df)

    # Upload Parquet to GCS
    upload_to_gcs(filename, 'p3dd-gcs-bucket', f'figi/bronze/{filename}')

    # Delete local file
    delete_local_file(filename)

if __name__ == '__main__':
    figi_update()