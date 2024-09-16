{{ config(
    materialized='incremental',
    unique_key=['day'],
    incremental_strategy='merge',
    properties={
        'format': "'PARQUET'",
        'partitioning': "array['year']"
    }
) }}

WITH daily_data AS (
    SELECT 
        year,
        month,
        day,
        resilience_level,
        sleep_recovery,
        daytime_recovery,
        stress,
        sleep_score,
        deep_sleep,
        efficiency,
        latency,
        rem_sleep,
        restfulness,
        timing,
        total_sleep,
        avg_awake_bpm,
        avg_rest_bpm,
        readiness_score,
        activity_balance,
        body_temperature,
        hrv_balance,
        recovery_index,
        resting_heart_rate,
        sleep_balance
    FROM {{ ref('daily_health') }}
),


monthly_aggregations AS (
    SELECT
        year,
        month,
        -- Main aggregated metrics
        AVG(CAST(stress AS DOUBLE)) AS avg_stress,
        AVG(CAST(sleep_score AS DOUBLE)) AS avg_sleep_score,
        AVG(CAST(avg_awake_bpm AS DOUBLE)) AS avg_awake_bpm,
        AVG(CAST(avg_rest_bpm AS DOUBLE)) AS avg_rest_bpm,
        AVG(CAST(readiness_score AS DOUBLE)) AS avg_readiness_score,       
        ARRAY_AGG(
            CAST(ROW(
                day,
                resilience_level,
                sleep_recovery,
                daytime_recovery,
                stress,
                sleep_score,
                deep_sleep,
                efficiency,
                latency,
                rem_sleep,
                restfulness,
                timing,
                total_sleep,
                avg_awake_bpm,
                avg_rest_bpm,
                readiness_score,
                activity_balance,
                body_temperature,
                hrv_balance,
                recovery_index,
                resting_heart_rate,
                sleep_balance
            )
            AS ROW(
                day DATE,
                resilience_level VARCHAR,
                sleep_recovery DECIMAL(10,0),
                daytime_recovery DECIMAL(10,0),
                stress DECIMAL(10,0),
                sleep_score DECIMAL(10,0),
                deep_sleep DECIMAL(10,0),
                efficiency DECIMAL(10,0),
                latency DECIMAL(10,0),
                rem_sleep DECIMAL(10,0),
                restfulness DECIMAL(10,0),
                timing DECIMAL(10,0),
                total_sleep DECIMAL(10,0),
                avg_awake_bpm DECIMAL(10,0),
                avg_rest_bpm DECIMAL(10,0),
                readiness_score DECIMAL(10,0),
                activity_balance DECIMAL(10,0),
                body_temperature DECIMAL(10,0),
                hrv_balance DECIMAL(10,0),
                recovery_index DECIMAL(10,0),
                resting_heart_rate DECIMAL(10,0),
                sleep_balance DECIMAL(10,0)
            ))
        ) AS daily_details
    FROM daily_data
    GROUP BY year, month)

SELECT *
FROM monthly_aggregations

{% if is_incremental() %}
WHERE (year > (SELECT MAX(year) FROM {{ this }}))
   OR (year = (SELECT MAX(year) FROM {{ this }}) AND month > (SELECT MAX(month) FROM {{ this }}))
{% endif %}
