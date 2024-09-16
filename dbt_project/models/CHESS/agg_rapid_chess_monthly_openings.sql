{{ config(
    materialized='incremental',
    unique_key=['year', 'month', 'pieces_color', 'opening'],
    incremental_strategy='merge',
    properties={
        'format': "'PARQUET'",
        'partitioning': "array['year', 'month']"
    }
) }}

WITH monthly AS (
    SELECT
        year,
        month,
        COALESCE(my_color, 'overall') AS pieces_color,
        COALESCE(opening, 'overall') AS opening,
        CAST(AVG(my_rating) AS DOUBLE) AS average_elo,
        COUNT(uuid) AS total_games,
        SUM(CASE WHEN my_result = 'win' THEN 1 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN opponent_result = 'win' THEN 1 ELSE 0 END) AS total_losses,
        SUM(CASE WHEN my_result <> 'win' AND opponent_result <> 'win' THEN 1 ELSE 0 END) AS total_draws
    FROM {{ ref('fct_chess_games') }}
    WHERE time_class = 'rapid'
    {% if is_incremental() %}
    AND (
        (year > (SELECT MAX(year) FROM {{ this }}))
        OR (year = (SELECT MAX(year) FROM {{ this }}) AND month >= (SELECT MAX(month) FROM {{ this }}))
    )
    {% endif %}
    GROUP BY GROUPING SETS(
        (year, month, my_color, opening),
        (year, month, my_color),
        (year, my_color, opening),
        (year,month),
        (year,my_color),
        (year)
    )
)

SELECT
    year,
    month,
    pieces_color,
    opening,
    CASE 
        WHEN opening = 'overall' and pieces_color = 'overall' THEN average_elo
        ELSE NULL
    END AS average_elo,
    total_games,
    total_wins,
    total_losses,
    total_draws
FROM monthly
order by year, month