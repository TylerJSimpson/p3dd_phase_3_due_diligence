# BigQuery SQL Details
External table and initial table creation in BigQuery.   

## nct
[nct.sql](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/bigquery_sql/nct.sql) - creates the **nct_external** table with the data from gcs created by the [nct_postgres_to_gcs_and_bq.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/nct_postgres_to_gcs.py) and then creates a managed table **nct** which contains the National Clinical Trial (nct) data that will be updated via truncate and write daily.  

## linkedin_jobs
[linkedin_jobs.sql](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/bigquery_sql/linkedin_jobs.sql) - creates the **linkedin_jobs_external** table with the data from gcs created by the [linkedin_to_gcs_and_bq.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs_and_bq.py) and then creates a managed table **linkedin_jobs**.  

## nct_source_mapping
[nct_source_mapping.sql](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/bigquery_sql/nct_source_mapping.sql) shows the schema for the **nct_source_mapping** table which links the nct and source tables. [nct_source_mapping.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/nct_source_mapping.py) pipeline populates the table [source] and [nct_id] fields.  
