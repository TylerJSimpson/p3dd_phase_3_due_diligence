{{ config(
    materialized='view',
    alias='jobs_monthly_growth'
) }}



SELECT  figi.figi_source_name,
        figi.ticker,
        AVG(prev.num_jobs) AS prev_week_avg_num_jobs,
        curr.num_jobs AS curr_day_num_jobs,
        CASE 
            WHEN AVG(prev.num_jobs) = 0 THEN NULL
            ELSE (curr.num_jobs - AVG(prev.num_jobs)) / AVG(prev.num_jobs)
        END AS growth_pct
FROM    `dtc-de-0315.bronze.jobs` curr
        JOIN `dtc-de-0315.bronze.jobs` prev
            ON curr.figi_primary_key = prev.figi_primary_key
            AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 30 DAY)
        JOIN `dtc-de-0315.bronze.figi` figi
            ON curr.figi_primary_key = figi.figi_primary_key
GROUP   BY figi.figi_source_name, figi.ticker, curr.num_jobs
ORDER   BY growth_pct DESC
