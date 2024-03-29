# Prefect
## Python Data Pipelines
Prefect is an orchestration framework that utilizes Python.  
It operates by assigning functions to tasks and calls them via a function assigned as the flow.  
Below you will find details on the python pipelines and the extract, transform, and load logic they contain as well as general setup instructions.  

## Pipelines

### [nct_postgres_to_gcs_and_bq.py](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/prefect_pipelines/nct_postgres_to_gcs_and_bq.py)
This pipeline extracts data from the [AACT database](https://aact.ctti-clinicaltrials.org/) and writes it to GCS and truncate and writes to the nct table in BigQuery.
### [nct_to_figi.py](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/prefect_pipelines/nct_to_figi.py)
This pipeline pulls the distinct [source] names from the nct table which act as unclean company names and writes them to the figi table in an upsert fashion where it detects new [source] names.
### [figi_update_to_gcs_and_bq.py](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/prefect_pipelines/figi_update_to_gcs_and_bq.py)
This pipeline cleans and parses the distinct [source] names through the [OpenFIGI API](https://www.openfigi.com/) and loads the data back into the figi table. This provides all of the financial identifiers such as ticker and cusip. It also adds the linkedin name that is used to parse linkedin in the jobs table.
### [jobs_linkedin_to_gcs_and_bq.py](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/prefect_pipelines/jobs_linkedin_to_gcs_and_bq.py)
This pipeline pulls the [linkedin_source_name] and parses Linkedin to return job data to append to the jobs table. Being the final pipeline this also conducts the run and refresh of all dbt models that promote up from bronze schema to silver and gold schemas.


## Setup

### Installation
Install Prefect:
```bash
pip install -U prefect
```

Ensure you are in the python environment:
```bash
conda activate zoom
```

If using a venv..  
Create venv:
```
python -m venv p3ddenv
```

Activate venv:
```
source p3ddenv/Scripts/activate
```

Authorize gcloud (if using GCP infrastructure):
```bash
gcloud auth application-default login
```

Run Prefect engine:
```
prefect orion start
```

If running a deployment the work queue must also be running in tandem with the Prefect engine:
```bash
prefect agent start --pool default-agent-pool --work-queue default
```

Port 4200 must also be forwarded to be able to connect to web UI via localhost:4200

### Interacting with Prefect

Flows can be ran manually if the Prefect engine is running:
```bash
python your_prefect_pipeline_name.py
```

Pipeline can be deployed with cron scheduling which requires the work queue to be running: 
Note when creating a build a .yaml file will be created.  
```bash
prefect deployment build ./your_prefect_pipeline_name.py:your_prefect_flow_name -n "your_chosen_deployment_name" --cron "30 3 * * *" -a
```
Confirm build deployment:  
```
prefect deployment apply your_chosen_deployment_name-deployment.yaml
```
