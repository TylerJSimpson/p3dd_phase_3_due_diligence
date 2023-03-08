### Storing credentials details in configuration file in INI format
```
[postgresql]
host = your_database_host
port = your_database_port
database = your_database_name
username = your_database_username
password = your_database_password

[gcs]
bucket = your_gcs_bucket
credentials_path = /path/to/your/gcs/credentials.json
```  

### Reading postgrsql credentials in a python pipeline
```python
import configparser

config = configparser.ConfigParser()
config.read('path/to/your/config.ini')

postgresql_credentials = {
    'host': config['postgresql']['host'],
    'port': config['postgresql']['port'],
    'database': config['postgresql']['database'],
    'username': config['postgresql']['username'],
    'password': config['postgresql']['password']
}

# Use these credentials to connect to your PostgreSQL database and extract data
```  

### Reading GCS credentials in a python pipeline
```python
gcs_bucket = config['gcs']['bucket']
gcs_credentials_path = config['gcs']['credentials_path']

# Use these values to authenticate with GCS and write data to the bucket
```  

### Example pipeline
```python
import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from configparser import ConfigParser

# Read the configuration file containing the credentials
config = ConfigParser()
config.read('path/to/your/config.ini')

# Extract the PostgreSQL credentials from the configuration file
pg_credentials = {
    'host': config['postgresql']['host'],
    'port': config['postgresql']['port'],
    'database': config['postgresql']['database'],
    'username': config['postgresql']['username'],
    'password': config['postgresql']['password']
}

# Extract the GCS credentials from the configuration file
gcs_credentials_path = config['gcs']['credentials_path']
gcs_bucket = config['gcs']['bucket']

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=pg_credentials['host'],
    port=pg_credentials['port'],
    database=pg_credentials['database'],
    user=pg_credentials['username'],
    password=pg_credentials['password']
)

# Query the database and fetch the data as a Pandas DataFrame
query = "SELECT * FROM your_table"
df = pd.read_sql_query(query, conn)

# Write the DataFrame to a Parquet file
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')

# Authenticate with GCS using the credentials file
client = storage.Client.from_service_account_json(gcs_credentials_path)

# Upload the Parquet file to GCS
bucket = client.get_bucket(gcs_bucket)
blob = bucket.blob('data.parquet')
blob.upload_from_filename('data.parquet')
```

