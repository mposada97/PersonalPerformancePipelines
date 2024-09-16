{{ config(
    materialized='incremental',
    unique_key='game_date',
    incremental_strategy='merge',
    properties={
        'format': "'PARQUET'",
        'partitioning': "array['year', 'month']"
    }
) }}

WITH daily_elo AS (
    SELECT
        year,
        month,
        game_date,
        CAST(AVG(my_rating) AS DOUBLE) AS average_elo,
        COUNT(uuid) AS total_games,
        SUM(CASE WHEN my_result = 'win' THEN 1 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN opponent_result = 'win' THEN 1 ELSE 0 END) AS total_losses,
        SUM(CASE WHEN my_result <> 'win' AND opponent_result <> 'win' THEN 1 ELSE 0 END) AS total_draws,
        ARRAY_AGG(
        CAST(
            ROW(
                uuid,
                game_date,
                my_rating,
                opponent_rating,
                my_result,
                opponent_result,
                number_of_rounds,
                my_remaining_time_seconds,
                opening
            ) AS ROW(
                game_uuid VARCHAR,
                game_date DATE,
                my_rating DOUBLE,
                opponent_rating DOUBLE,
                my_result VARCHAR,
                opponent_result VARCHAR,
                number_of_rounds DOUBLE,
                my_remaining_time_seconds DOUBLE,
                opening VARCHAR
            )
        )
    ) AS game_details
    FROM {{ ref('fct_chess_games') }}
    WHERE time_class = 'rapid'
    {% if is_incremental() %}
    AND game_date >= (SELECT MAX(game_date) FROM {{ this }})
    {% endif %}
    GROUP BY year, month, game_date, time_class
)

SELECT
    year,
    month,
    game_date,
    average_elo,
    total_games,
    total_wins,
    total_losses,
    total_draws,
    game_details
FROM daily_elo
ORDER BY game_date