# Phase 3 Clinical Trials Project
### Overview:   
### Companies with clinical trials in phase 3 are high risk high reward investments. This project aims to mitigate this risk by compiling data for phase 3 clinical trials (ClinicalTrails.gov) with job posting details (LinkedIn.com) for the tracking of company job activity as a potential early indicator of trial success.
![Flowchart](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/Flowchart.jpg)
___
### Stage 1: Clinical Trial Data
* **EXTRACT:** Utilize [AACT PostgreSQL database](https://aact.ctti-clinicaltrials.org/) which populates data from ClinicalTrials.gov and interface with Prefect Python pipeline
* **LOAD:** Prefect pipeline loads the data into GCS and BigQuery landing zones
### Stage 2: Job Data
* **EXTRACT:** Prefect pipeline queries Clinical Trial Data and parses companies to LinkedIn to extract the number of jobs for each company  
* **LOAD:** Prefect pipeline loads the data into GCS and BigQuery landing zones
### Stage 3: Serve Data
### Stage 4: ETL Sentiment Data

___
### Tools:
#### * Programming: Python, SQL
#### * ETL: Prefect, DBT
#### * GCP: GCS, BigQuery
#### * Serving: Looker

