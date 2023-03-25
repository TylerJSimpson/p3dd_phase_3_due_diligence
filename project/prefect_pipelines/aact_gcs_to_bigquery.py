from google.cloud import bigquery
from google.cloud import storage
from prefect import task, Flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import datetime

"""
TO-DO:
add block architecture for BigQuery and GCS
add logging
"""

@task
def retrieve_latest_file(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    latest_blob = None
    for blob in blobs:
        if not latest_blob or blob.updated > latest_blob.updated:
            latest_blob = blob

    return latest_blob


@task
def load_to_bigquery(file_uri, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]

    # Configure the merge operation
    merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` AS target
        USING (
            SELECT * FROM `{project_id}.{dataset_id}.{table_id}_temp`
        ) AS source
        ON target.nct_id = source.nct_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """

    # Start the load job
    load_job = client.load_table_from_uri(
        file_uri,
        f"{project_id}.{dataset_id}.{table_id}_temp",
        job_config=job_config,
    )

    load_job.result()

    # Execute the merge operation
    merge_job = client.query(merge_query)

    return f"{project_id}.{dataset_id}.{table_id}"


@Flow
def gcs_to_bigquery():

    latest_blob = retrieve_latest_file('zoom-gcs', 'project/bronze/aact_studies_*')
    file_uri = f"gs://{latest_blob.bucket.name}/{latest_blob.name}"

    table_id = "aact_studies"
    project_id = "dtc-de-0315"
    dataset_id = "bronze"

    load_to_bigquery(file_uri, project_id, dataset_id, table_id)

    delete_local_file(parquet_file)


if __name__ == "__main__":
    gcs_to_bigquery()
