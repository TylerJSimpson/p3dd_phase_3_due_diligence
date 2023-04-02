CREATE OR REPLACE EXTERNAL TABLE `dtc-de-0315.bronze.aact_studies_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-0315/project/bronze/aact_studies_03212023_235654.parquet'] --note this can change just using this as an example to get schema
);

CREATE OR REPLACE TABLE `dtc-de-0315.bronze.aact_studies`
#PARTITION BY left out due to too high of cardinality on date fields and partition not being an int which can be fixed if necessary
CLUSTER BY nct_id
AS
SELECT  *
FROM    `dtc-de-0315.bronze.aact_studies_external`
; 
/*
TO DO - test and optimize partition and cluster
Test queries that ran in 10 secs prior to CLUSTER BY nct_id ran in 10sec and now ran in 6sec
*/
