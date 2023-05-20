from prefect import Flow, task
from google.cloud import bigquery
import requests
import configparser
import time

@task
def read_bq_nct():
    """
    Queries BigQuery table bronze.figi
    Returns unique identifier (figi_primary_key) and company name from nct data (nct_source_name)
    from the figi table where it has not yet been identified
    """

    # Instantiate the BigQuery client
    client = bigquery.Client(project="dtc-de-0315")

    # Define the query
    query = """
        SELECT  figi_primary_key,
                nct_source_name
        FROM    `dtc-de-0315.bronze.figi`
        WHERE   linkedin_source_name IS NULL
        AND     end_date IS NULL
    """

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()

    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()

    # Apply the transformations to the nct_source_name column
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
    # Read config file
    config = configparser.ConfigParser()
    config.read('../configuration/config.ini')

    # Get the API key from the config file
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

    # Access the response data
    adr_response_data = adr_response.json()
    common_stock_response_data = common_stock_response.json()

    print("ADR Response:")
    print(adr_response_data)
    print("Common Stock Response:")
    print(common_stock_response_data)

    # Add a wait function to respect rate limits
    time.sleep(3)  # Wait for 3 seconds before making the next API call

@Flow
def figi_update():
    # Queries BigQuery and writes to pandas dataframe
    data = read_bq_nct()
    
    # Call OpenFIGI API for each query in nct_source_name_cleaned
    for query in data['nct_source_name_cleaned']:
        call_openfigi_api(query)
        
        # Add a wait function to respect rate limits
        time.sleep(15)  # Wait for 15 seconds before making the next batch of API calls


if __name__ == '__main__':
    figi_update()