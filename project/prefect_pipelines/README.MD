# Prefect Pipelines Details
Generalized instructions for setup and running the prefect pipelines for the clinicaltrials_2023 project.   

## Setup

Ensure you are in the python environment:
```bash
python activate zoom
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

### aact_postgres_to_gcs.py
This pipeline moves data from the public AACT database to GCS bucket.  
[aact_postgres_to_gcs.py](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/aact_postgres_to_gcs.py)  
[postgresql_to_gcs-deployment.yaml](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/project/prefect_pipelines/postgresql_to_gcs-deployment.yaml)  
  
run flow manually locally:
```bash
python aact_postgres_to_gcs.py
```
  
### Deployment  
.yaml file created automatically when creating build:
```bash
prefect deployment build ./aact_postgres_to_gcs.py:postgresql_to_gcs -n "aact-postgres-to-gcs.py"
```
Confirm build deployment:  
```
prefect deployment apply postgresql_to_gcs_deployment.yaml
```
