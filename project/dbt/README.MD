# dbt
## SQL Transformations
dbt is a tool that allows for transformations, testing, and documentation of data and data structures using SQL.  
Below you will find details on the dbt models used in this project as well as general setup instructions.  

## Models
### Views
#### Schema: Silver  

name: jobs_daily_total  
description: "Aggregates the daily number of job postings for each Financial Instrument   Global Identifier (FIGI). Excludes FIGIs with any day where the number of job postings is greater than or equal to 1000."  
alias: jobs_daily_total  
materialized: view  
schema: silver  

name: jobs_daily_growth  
description: "Calculates the daily percentage growth in the number of job postings for each Financial Instrument Global Identifier (FIGI). Excludes FIGIs with any day where the number of job postings is greater than or equal to 1000."  
alias: jobs_daily_growth  
materialized: view  
schema: silver  

name: jobs_weekly_growth  
description: "Calculates the weekly percentage growth in the number of job postings for each Financial Instrument Global Identifier (FIGI). Excludes FIGIs with any day where the number of job postings is greater than or equal to 1000."  
alias: jobs_weekly_growth  
materialized: view  
schema: silver  

name: jobs_monthly_growth  
description: "Calculates the monthly percentage growth in the number of job postings for each Financial Instrument Global Identifier (FIGI). Excludes FIGIs with any day where the number of job postings is greater than or equal to 1000."  
alias: jobs_monthly_growth  
materialized: view  
schema: silver  




## Setup

### Installation
Install Prefect:
```bash
pip install dbt-bigquery
```
Check version:
```bash
dbt --version
```
Create a directory ie 'dbt':
```bash
mkdir dbt
```
Move to directory.  
Initialize dbt:
```bash
dbt init
```
This will prompt you to name your dbt project and ask for details based on the database you choose.


### Interacting with dbt

Initialize, test, and run with specific profiles.yml path. Must be ran from the directory containing the dbt_project.yml:  
```bash
dbt init --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt
```
```bash
dbt test --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt
```
```bash
dbt run --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt
```

When using multiple target datasets (schemas) you will have to specify models and targets on the fly in separate bash scripts:
```bash
dbt run --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt --target silver --models s_t_jobs_growth --full-refresh
```
--full-refresh forces a full drop and recreate:  
```bash
dbt run --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt --target gold --models g_v_jobs_daily_total g_v_jobs_daily_weekly_monthly_growth --full-refresh
```