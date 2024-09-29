{{ config(
    materialized='incremental',
    unique_key='uuid',
     properties={
        'format': "'PARQUET'",
        'partitioning': "array['year', 'month']"
    }
) }}

with chess_games as (
    SELECT
    uuid,
    year,
    month,
    game_date,
    end_time,
    time_class,
    time_control,
    CASE 
        WHEN white_username = 'mposada97' THEN 'white' 
        ELSE 'black' 
    END AS my_color,
    CASE 
        WHEN white_username = 'mposada97' THEN white_rating 
        ELSE black_rating 
    END AS my_rating,
    CASE 
        WHEN white_username = 'mposada97' THEN black_username 
        ELSE white_username 
    END AS opponent_username,
    CASE 
        WHEN white_username = 'mposada97' THEN black_rating 
        ELSE white_rating 
    END AS opponent_rating,
    CASE 
        WHEN white_username = 'mposada97' THEN white_first_move 
        ELSE black_first_move 
    END AS my_first_move,
    CASE 
        WHEN white_username <> 'mposada97' THEN white_first_move 
        ELSE black_first_move 
    END AS opponent_first_move,
    CASE 
        WHEN white_username = 'mposada97' THEN 
            CAST(split(white_remaining_time, ':')[1] AS DOUBLE) * 3600 + 
            CAST(split(white_remaining_time, ':')[2] AS DOUBLE) * 60 + 
            CAST(split(white_remaining_time, ':')[3] AS DOUBLE)
        ELSE 
            CAST(split(black_remaining_time, ':')[1] AS DOUBLE) * 3600 + 
            CAST(split(black_remaining_time, ':')[2] AS DOUBLE) * 60 + 
            CAST(split(black_remaining_time, ':')[3] AS DOUBLE)
    END AS my_remaining_time_seconds,
    CASE 
        WHEN white_username = 'mposada97' THEN 
            CAST(split(black_remaining_time, ':')[1] AS DOUBLE) * 3600 + 
            CAST(split(black_remaining_time, ':')[2] AS DOUBLE) * 60 + 
            CAST(split(black_remaining_time, ':')[3] AS DOUBLE)
        ELSE 
            CAST(split(white_remaining_time, ':')[1] AS DOUBLE) * 3600 + 
            CAST(split(white_remaining_time, ':')[2] AS DOUBLE) * 60 + 
            CAST(split(white_remaining_time, ':')[3] AS DOUBLE)
    END AS opponent_remaining_time_seconds,
    CASE 
        WHEN white_username = 'mposada97' THEN white_result 
        ELSE black_result 
    END AS my_result,
    CASE 
        WHEN white_username <> 'mposada97' THEN white_result 
        ELSE black_result 
    END AS opponent_result,
    number_of_rounds,
    termination,
    regexp_extract(opening, '[^/]+$') AS opening

    from {{ source('mposada' , 'chess_games') }}
)

-- This part will run for both incremental and full-refresh
select
    *
from chess_games

{% if is_incremental() %}

where end_time > (select max(end_time) from {{ this }})

{% endif %}
