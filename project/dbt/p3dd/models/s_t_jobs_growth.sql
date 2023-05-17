{{ config(
    materialized='table',
    alias='jobs_growth'
) }}

SELECT  figi_primary_key,
        nct_source_name,
        ticker,
        MAX(IF(DATE(timestamp) = max_date, num_jobs, NULL)) AS curr_day_num_jobs,
        MAX(IF(DATE(timestamp) = prev_date, num_jobs, NULL)) AS prev_day_num_jobs,
        AVG(IF(DATE(timestamp) BETWEEN prev_week_start AND prev_week_end, num_jobs, NULL)) AS prev_week_num_jobs,
        AVG(IF(DATE(timestamp) BETWEEN prev_month_start AND prev_month_end, num_jobs, NULL)) AS prev_month_num_jobs
FROM
  (
    SELECT  jobs.figi_primary_key,
            figi.nct_source_name,
            figi.ticker,
            num_jobs,
            timestamp,
            MAX(DATE(timestamp)) OVER () AS max_date,
            DATE_SUB(MAX(DATE(timestamp)) OVER (), INTERVAL 1 DAY) AS prev_date,
            DATE_SUB(MAX(DATE(timestamp)) OVER (), INTERVAL 7 DAY) AS prev_week_start,
            DATE_SUB(MAX(DATE(timestamp)) OVER (), INTERVAL 1 DAY) AS prev_week_end,
            DATE_SUB(MAX(DATE(timestamp)) OVER (), INTERVAL 30 DAY) AS prev_month_start,
            DATE_SUB(MAX(DATE(timestamp)) OVER (), INTERVAL 1 DAY) AS prev_month_end
    FROM    `dtc-de-0315.bronze.jobs` jobs
            JOIN `dtc-de-0315.bronze.figi` figi ON jobs.figi_primary_key = figi.figi_primary_key
  )
GROUP BY  figi_primary_key, nct_source_name, ticker
HAVING    curr_day_num_jobs < 1000 OR
          prev_day_num_jobs < 1000 OR
          prev_week_num_jobs < 1000 OR
          prev_month_num_jobs < 1000

