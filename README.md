# Phase 3 Clinical Trials Project (Dec 22 - Present)
### Overview:   
### Clinical trials in phase 3 are very risky investments. In order to help mitigate the risk this project aims to compile data for phase 3 clinical trials (ClinicalTrails.gov) with job posting details (LinkedIn) and sentiment (Twitter) to better inform investors.
![Flowchart](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/Flowchart_ClinicalDataProject.jpg)
___
### Stage 1: ETL Clinical Data
* RESEARCH: Determine necessary data fields needed from ClinicalTrials.gov
* EXTRACT: Utilize [AACT PostgreSQL database](https://aact.ctti-clinicaltrials.org/) which populates data from ClinicalTrials.gov daily to dump the necessary fields
* EXTRACT: Prefect Python pipeline to load raw data from Postgres into GCS
* LOAD: Prefect Python pipeline to load raw data from GCS to BigQuery
* TRANFORM: DBT SQL transformation in BigQuery data staged for merging with job data
### Stage 2: ETL Job Data
* RESEARCH: Determine necessary data fields needed from LinkedIn.com
* EXTRACT: Prefect Python pipeline to scrape raw data from LinkedIn into GCS
* LOAD: Prefect Python pipeline to load raw data from GCS to BigQuery
* TRANSFORM: DBT SQL transformation in BigQuery data staged for merging with clinical data
### Stage 3: Serve Data
### Stage 4: ETL Sentiment Data

___
### Tools:
#### * Programming: Python, SQL
#### * ETL: Prefect, DBT
#### * Cloud: GCP - GCS, BigQuery
#### * Serving: Placeholder

