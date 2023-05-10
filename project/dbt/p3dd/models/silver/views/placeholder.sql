CREATE MATERIALIZED VIEW `dtc-de-0315.bronze.jobs_weekly_growth`
AS
SELECT
  curr.figi_primary_key,
  prev.num_jobs AS prev_num_jobs,
  curr.num_jobs AS curr_num_jobs,
  CASE 
    WHEN prev.num_jobs = 0 THEN null
    ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
  END AS growth_pct
FROM
  `dtc-de-0315.bronze.jobs_copy` curr
JOIN
  `dtc-de-0315.bronze.jobs_copy` prev
ON
  curr.figi_primary_key = prev.figi_primary_key
  AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 7 DAY)
ORDER BY
  growth_pct DESC
LIMIT
  25;

CREATE MATERIALIZED VIEW `dtc-de-0315.bronze.jobs_monthly_growth`
AS
SELECT
  curr.figi_primary_key,
  prev.num_jobs AS prev_num_jobs,
  curr.num_jobs AS curr_num_jobs,
  CASE 
    WHEN prev.num_jobs = 0 THEN null
    ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
  END AS growth_pct
FROM
  `dtc-de-0315.bronze.jobs_copy` curr
JOIN
  `dtc-de-0315.bronze.jobs_copy` prev
ON
  curr.figi_primary_key = prev.figi_primary_key
  AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 30 DAY)
ORDER BY
  growth_pct DESC
LIMIT
  25;

CREATE MATERIALIZED VIEW `dtc-de-0315.bronze.jobs_daily_loss`
AS
SELECT
  curr.figi_primary_key,
  prev.num_jobs AS prev_num_jobs,
  curr.num_jobs AS curr_num_jobs,
  CASE 
    WHEN prev.num_jobs = 0 THEN null
    ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
  END AS growth_pct
FROM
  `dtc-de-0315.bronze.jobs_copy` curr
JOIN
  `dtc-de-0315.bronze.jobs_copy` prev
ON
  curr.figi_primary_key = prev.figi_primary_key
  AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 1 DAY)
WHERE
  prev.num_jobs > 0 AND curr.num_jobs < prev.num_jobs
ORDER BY
  growth_pct ASC
LIMIT
  25;

CREATE MATERIALIZED VIEW `dtc-de-0315.bronze.jobs_weekly_loss`
AS
SELECT
  curr.figi_primary_key,
  prev.num_jobs AS prev_num_jobs,
  curr.num_jobs AS curr_num_jobs,
  CASE 
    WHEN prev.num_jobs = 0 THEN null
    ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
  END AS growth_pct
FROM
  `dtc-de-0315.bronze.jobs_copy` curr
JOIN
  `dtc-de-0315.bronze.jobs_copy` prev
ON
  curr.figi_primary_key = prev.figi_primary_key
  AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 7 DAY)
WHERE
  prev.num_jobs > 0 AND curr.num_jobs < prev.num_jobs
ORDER BY
  growth_pct ASC
LIMIT
  25;

CREATE MATERIALIZED VIEW `dtc-de-0315.bronze.jobs_monthly_loss`
AS
SELECT
  curr.figi_primary_key,
  prev.num_jobs AS prev_num_jobs,
  curr.num_jobs AS curr_num_jobs,
  CASE 
    WHEN prev.num_jobs = 0 THEN null
    ELSE (curr.num_jobs - prev.num_jobs) / prev.num_jobs
  END AS growth_pct
FROM
  `dtc-de-0315.bronze.jobs_copy` curr
JOIN
  `dtc-de-0315.bronze.jobs_copy` prev
ON
  curr.figi_primary_key = prev.figi_primary_key
  AND DATE(curr.timestamp) = DATE_SUB(DATE(prev.timestamp), INTERVAL 30 DAY)
WHERE
  prev.num_jobs > 0 AND curr.num_jobs < prev.num_jobs
ORDER BY
  growth_pct ASC
LIMIT
  25;