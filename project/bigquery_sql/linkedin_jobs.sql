CREATE OR REPLACE EXTERNAL TABLE `dtc-de-0315.bronze.linkedin_jobs_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-0315/project/bronze/linkedin_jobs/linkedin_jobs_04022023_194438.parquet'] --note this can change just using this as an example to get schema
);

CREATE OR REPLACE TABLE `dtc-de-0315.bronze.linkedin_jobs`
AS
SELECT  *
FROM    `dtc-de-0315.bronze.aact_studies_external`
; 
