{{ config(
    materialized='view',
    alias='jobs_daily_weekly_monthly_growth'
) }}

SELECT
  figi_primary_key,
  nct_source_name,
  ticker,
  curr_day_num_jobs,
  prev_day_num_jobs,
  prev_week_num_jobs,
  prev_month_num_jobs,
  (curr_day_num_jobs - prev_day_num_jobs) / NULLIF(prev_day_num_jobs, 0) AS daily_growth,
  (curr_day_num_jobs - prev_week_num_jobs) / NULLIF(prev_week_num_jobs, 0) AS weekly_growth,
  (curr_day_num_jobs - prev_month_num_jobs) / NULLIF(prev_month_num_jobs, 0) AS monthly_growth
FROM
  `dtc-de-0315.silver.jobs_growth`
