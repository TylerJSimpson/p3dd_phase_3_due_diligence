# dbt
## SQL Transformations
dbt is a tool that allows for transformations, testing, and documentation of data and data structures using SQL.  
Below you will find details on the dbt models used in this project as well as general setup instructions.  

## Models

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

To initialize as above and test with specific profile directory path:
```bash
dbt init --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt
```
```bash
dbt test --profiles-dir /c/Users/simps/p3dd_phase_3_due_diligence/project/dbt
```