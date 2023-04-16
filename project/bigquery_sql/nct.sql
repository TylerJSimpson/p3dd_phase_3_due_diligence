/*
nct.sql
*/

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-0315.bronze.nct_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-0315/project/bronze/aact_studies/aact_studies_04142023_135439.parquet'] --note this can change just using this as an example to get schema
);

CREATE OR REPLACE TABLE `dtc-de-0315.bronze.nct`
#PARTITION BY left out due to too high of cardinality on date fields and partition not being an int which can be fixed if necessary
CLUSTER BY nct_id
AS
SELECT  *
FROM    `dtc-de-0315.bronze.nct_external`
;