

  create or replace view `dtc-de-0315`.`gold`.`jobs_daily_total`
  OPTIONS()
  as 

SELECT  DATE(DATE_TRUNC(timestamp, DAY)) AS day,
        SUM(num_jobs) AS total_num_jobs
FROM    `dtc-de-0315.bronze.jobs`
WHERE   figi_primary_key NOT IN (
            SELECT figi_primary_key
            FROM `dtc-de-0315.bronze.jobs`
            WHERE num_jobs >= 1000
            GROUP BY figi_primary_key
        )
GROUP   BY day
HAVING  MAX(num_jobs) < 1000  
ORDER   BY day DESC;

