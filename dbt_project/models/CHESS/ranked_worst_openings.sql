{{ config(
    materialized='table',
    properties={
        'format': "'PARQUET'"
    }
) }}

WITH ranked_worst AS (
  SELECT 
    pieces_color, 
    opening, 
    win_ratio,
    ROW_NUMBER() OVER (PARTITION BY pieces_color ORDER BY win_ratio ASC) AS rank
  FROM 
    mposada.agg_rapid_chess_monthly_openings
  WHERE 
    month IS NULL 
    AND pieces_color <> 'overall' 
    AND opening <> 'overall'
    AND total_games >= CAST((
      SELECT COUNT(*) 
      FROM mposada.fct_chess_games 
      WHERE time_class = 'rapid'
    ) * 0.01 AS INTEGER)
)
SELECT *
FROM ranked_worst
WHERE rank <= 5