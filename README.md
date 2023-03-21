# Phase 3 Clinical Trials Project (Dec 22 - Present)
### Overview:   
### Clinical trials in phase 3 are risky investments to navigate. This project aims to mitigate this risk by compiling data for phase 3 clinical trials (ClinicalTrails.gov) with job posting details (LinkedIn.com) for the tracking of company job activity as a potential early indicator of trial success.
![Flowchart](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/Flowchart_Project.jpg)
___
### Stage 1: ETL Clinical Data
* **RESEARCH:** Determine necessary data fields needed from ClinicalTrials.gov
* **EXTRACT:** Utilize [AACT PostgreSQL database](https://aact.ctti-clinicaltrials.org/) which populates data from ClinicalTrials.gov daily to dump the necessary fields
* **EXTRACT:** Prefect Python pipeline to load raw data from Postgres into GCS bronze landing zone
* **LOAD:** DBT SQL external table to load raw data from GCS to BigQuery
* **TRANFORM:** DBT SQL transformation in BigQuery data staged for merging with job data. The companies names will be passed as variables from the job data pipeline
### Stage 2: ETL Job Data
* **RESEARCH:** Determine necessary data fields needed from LinkedIn.com
* **EXTRACT:** Prefect Python pipeline to scrape raw data from LinkedIn into GCS bronze landing zone
* **LOAD:** DBT SQL external table to load raw data from GCS to BigQuery 
* **TRANSFORM:** DBT SQL transformations in BigQuery data staged for merging with clinical data
### Stage 3: Serve Data
### Stage 4: ETL Sentiment Data

___
### Tools:
#### * Programming: Python, SQL
#### * ETL: Prefect, DBT
#### * GCP: GCS, BigQuery
#### * Serving: Looker

