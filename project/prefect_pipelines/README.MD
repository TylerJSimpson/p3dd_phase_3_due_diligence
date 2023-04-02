# Prefect Pipelines Details
Generalized instructions for setup and running the prefect pipelines for the clinicaltrials_2023 project.   

## Setup

Ensure you are in the python environment:
```bash
conda activate zoom
```

Authorize gcloud:
```bash
gcloud auth application-default login
```

Prefect must be running:
```
prefect orion start
```

If running a deployment the work queue must also be running in tandem with above:
```bash
prefect agent start  --work-queue "default"
```

Port 4200 must also be forwarded to be able to connect to web UI via localhost:4200

## Pipelines

### aact_postgres_to_gcs_and_bq.py
This pipeline moves data from the public AACT database to GCS bucket and BigQuery.  
[aact_postgres_to_gcs_and_bq.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs_and_bq.py)  
[aact_postgres_to_gcs_and_bq.yaml](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs_and_bq.yaml)  
  
run flow manually locally:
```bash
python aact_postgres_to_gcs_and_bq.py
```
  
#### Deployment  
Scheduling build for 3:00 AM daily as the AACT database refreshes at 12:00 AM daily except Saturday which is a ~24 hours refresh but the data is available during this time.  
.yaml file created automatically when creating build:
```bash
prefect deployment build ./aact_postgres_to_gcs_and_bq.py:aact_postgres_to_gcs_and_bq -n "aact_postgres_to_gcs_and_bq" --cron "0 3 * * *" -a
```
Confirm build deployment:  
```
prefect deployment apply aact_postgres_to_gcs_and_bq.yaml
```

### linkedin_to_gcs_and_bq.py
PLACEHOLDER.  
[linkedin_to_gcs_and_bq.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/linkedin_to_gcs_and_bq.py)  
[linkedin_to_gcs_and_bq.py.yaml](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs_and_bq.yaml)  
  
run flow manually locally:
```bash
python aact_postgres_to_gcs_and_bq.py
```
  
#### Deployment  
Scheduling build for 3:30 AM daily as the previous pipeline runs at 3:00 AM.  
.yaml file created automatically when creating build:
```bash
prefect deployment build ./linkedin_to_gcs_and_bq.py:linkedin_to_gcs_and_bq -n "linkedin_to_gcs_and_bq" --cron "30 3 * * *" -a
```
Confirm build deployment:  
```
prefect deployment apply aact_postgres_to_gcs_and_bq-deployment.yaml
```
