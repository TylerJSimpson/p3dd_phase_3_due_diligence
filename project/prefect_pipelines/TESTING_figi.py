from prefect import Flow, task
from google.cloud import bigquery
import requests
import configparser
import time
import pandas as pd
from datetime import datetime

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
        LIMIT   4
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
def write_dataframe_to_csv(df, filename):
    df.to_csv(filename, index=False)


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

    write_dataframe_to_csv(combined_df, 'combined_data.csv')


if __name__ == '__main__':
    figi_update()