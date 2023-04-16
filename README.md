# Phase 3 Due Diligence (P3DD) 
### Companies with clinical trials in phase 3 are high risk high reward investments. P3DD aims to mitigate this risk by compiling data for phase 3 clinical trials with job posting details for the tracking of company job activity as a potential early indicator of trial success.
![Flowchart](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/Flowchart.jpg)
___
### National Clinical Trial data (nct)
#### Utilize [AACT PostgreSQL database](https://aact.ctti-clinicaltrials.org/) which populates data from ClinicalTrials.gov.
Pipeline(s):
* placeholder
Table(s):
* placeholder

### Company data (source)
#### Utilize [OpenFIGI](https://www.openfigi.com/) which contains company data based on their Financial Instrument Global Identifier (FIGI).
Pipeline(s):
* placeholder
Table(s):
* placeholder

### nct to source mapping
#### Manual mapping used to map the source field (unclean company name) from the nct data with the name field (clean company name) from the source FIGI data.
Pipeline(s):
* placeholder
Table(s):
* placeholder

### jobs data
#### Utilize Linkedin to parse company names and compile job details.
Pipeline(s):
* placeholder
Table(s):
* placeholder

___
### Tools:
#### * Programming: Python, SQL
#### * ETL: Prefect, DBT
#### * GCP: GCS, BigQuery
#### * Serving: Looker

