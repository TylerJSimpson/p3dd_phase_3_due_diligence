# BigQuery SQL Details
External table and initial table creation in BigQuery for clinicaltrials_2023 project.   

## aact_studies
[aact_studies.sql](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/bigquery_sql/aact_studies.sql) - creates the **aact_studies_external** table with the data from gcs created by the [aact_postgres_to_gcs.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs.py) and then creates a managed table **aact_studies** which will be used by dbt.
