{{ config(
    materialized='view',
    alias='jobs_daily_growth'
) }}

SELECT  figi.figi_source_name,
        figi.ticker,
        prev.num_jobs AS prev_day_num_jobs,
        curr.num_jobs AS curr_day_num_jobs,
        CASE 
          WHEN prev.num_jobs = 0 THEN NULL
          ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
        END AS growth_pct
FROM    `dtc-de-0315.bronze.jobs` curr
        JOIN `dtc-de-0315.bronze.jobs` prev
          ON curr.figi_primary_key = prev.figi_primary_key
          AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 1 DAY)
        JOIN `dtc-de-0315.bronze.figi` figi
          ON curr.figi_primary_key = figi.figi_primary_key
GROUP   BY figi.figi_source_name, figi.ticker, prev.num_jobs, curr.num_jobs
HAVING  MAX(curr.num_jobs) < 1000
ORDER   BY growth_pct DESC

