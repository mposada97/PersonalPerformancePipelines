{{ config(
    materialized='incremental',
    unique_key=['day'],
    incremental_strategy='merge',
    properties={
        'format': "'PARQUET'",
        'partitioning': "array['year', 'month']"
    }
) }}

WITH resilience AS (
SELECT *
FROM {{ source('mposada' , 'resilience') }}),

sleep AS (
SELECT *
FROM {{ source('mposada' , 'sleep_data') }}),

heart_rate AS (
SELECT 
local_day,
AVG(CASE WHEN source = 'awake' THEN (CAST(bpm AS DOUBLE)) END) AS avg_awake_bpm,
AVG(CASE WHEN source = 'rest' THEN (CAST(bpm AS DOUBLE)) END) AS avg_rest_bpm
FROM {{ source('mposada' , 'heart_rate') }}
GROUP BY local_day
),

readiness AS (
SELECT *
FROM {{ source('mposada' , 'readiness') }}),

daily_stats AS
(
SELECT 
  COALESCE(slp.year, res.year, year(hr.local_day), rds.year) AS year,
  COALESCE(slp.month, res.month, month(hr.local_day), rds.month) AS month,
  COALESCE(slp.day, res.day, hr.local_day, rds.day) AS day,
  res.level AS resilience_level,
  res.sleep_recovery,
  res.daytime_recovery, 
  res.stress,
  slp.score AS sleep_score,
  slp.deep_sleep,
  slp.efficiency,
  slp.latency,
  slp.rem_sleep,
  slp.restfulness,
  slp.timing,
  slp.total_sleep,
  hr.avg_awake_bpm,
  hr.avg_rest_bpm,
  rds.score AS readiness_score,
  rds.activity_balance,
  rds.body_temperature,
  rds.hrv_balance,
  rds.recovery_index,
  rds.resting_heart_Rate,
  rds.sleep_balance
FROM resilience res
FULL JOIN sleep slp ON slp.day = res.day
FULL JOIN heart_rate hr ON hr.local_day = COALESCE(res.day, slp.day)
FULL JOIN readiness rds ON rds.day = COALESCE(res.day, slp.day, hr.local_day)
)
select
    *
from daily_stats

{% if is_incremental() %}

-- This part will only run when dbt run is used with the --full-refresh option
where day >= (select max(day) from {{ this }})

{% endif %}