# Personal Performance Pipelines

The idea of this project was to build data pipelines around my personal data to help me track where I'm at in different aspects of my life and find areas of improvement. The first step was this diagram which I used to decide which aspects of my life I am interested in tracking and which have readily available data that I can consume through API's.

![image](https://github.com/user-attachments/assets/40ff19d1-5047-449e-9882-c4e1bb553c5a)

After completing the diagram I decided to leverage IoT techonlogy with my Oura ring to get my health data that includes readiness, sleep, resilience and heart rate. I also decided to build a pipeline around my newest hobbie, which is chess, I have been playing games in chess.com, they have a public API that I can consume to get my games.

# Scope
- Create a daily batch pipeline for my health data (from my oura ring).
- Create a monthly batch pipeline for my chess games (from chess.com public API).
  - API's will be consumed using spark, dbt will be used for further modeling.
- Create data visualizations to give me more visibility to my health trends.
- Create data visualizations to track my ELO, and analyze which are my strengths and weaknesses when it comes to chess openings and pieces colors (for example: do I play better with white or black pieces? which openings should I study further to increase my win rate when those openings are played by my oponent, which openings should I keep playing myself?).

# Data Stack
This project is built on DataExpert.io's infraestructure (therefore, I only copied my scripts and DAGs to this repo, all of the aws setup is left outside of this public repo to protect the bootcamps infrastructure).
Storage: AWS S3
Metadata: Iceberg
Query Engine: Trino
Orchestrator: Airflow (Astro)
Transformations, tests and documentation: dbt
Data Visualization: Superset

# Data Modeling / Data Dictionary
## Chess

These are the tables I designed for my chess games, I first consumed the API using spark and processed it into a fact table, then using dbt I created agg_rapid_chess_daily which is an aggregate by day of the results of my chess games, it also includes a column with a struct array that includes details of every chess game played that day, this table is not going to be used in my analysis but could be used for further more detailed analysis. I also creeated agg_rapid_chess_monthly_openings, this is an aggregated table that uses grouping sets to provide easy quering to analyze different aspects of my chess games by year and by month, it also shows how I performed with different color pieces and with different openings as well as overall performance (all colors and all openings). From my monthly chess games aggregate I also created two more tables, ranked_best_openings and ranked_worst_openings, these tables contain my top 5 and worst 5 openings (5 per each color), I was not planning on creating these two tables at first, but the ordering of my bars in the superset barchart wasnt working when I created the table out of of my monthly aggregation table, the ordering worked well with this tables and it also made the refresh of the vizualizations in superset faster.

Note: You will notice that I experimented with doubles and decimals in my tables. I understand that it is better to stick to one for consistency but I wanted to experiment with both, I ended liking more the doubles and will be sticking to them in the future but i didnt go back to update all tables for the purposes of this project.

This is a link to the API documentation if you want to attempt a similar project:
https://www.chess.com/news/view/published-data-api

### fct_chess_games

| Column Name                       | Data Type | Description                                                                                |
|-----------------------------------|-----------|--------------------------------------------------------------------------------------------|
| uuid                              | VARCHAR   | Unique identifier for each chess game                                                      |
| year                              | INTEGER   | Year the game was played                                                                   |
| month                             | INTEGER   | Month the game was played                                                                  |
| game_date                         | DATE      | Date the game was played                                                                   |
| end_time                          | TIMESTAMP | Time when the game ended                                                                   |
| time_class                        | VARCHAR   | Class of the game (e.g., rapid, blitz, bullet)                                             |
| time_control                      | VARCHAR   | Time control settings for the game                                                         |
| my_color                          | VARCHAR   | Color of pieces played by 'mposada97' ('white' or 'black')                                 |
| my_rating                         | INTEGER   | Rating of 'mposada97' for this game                                                        |
| opponent_username                 | VARCHAR   | Username of the opponent                                                                   |
| opponent_rating                   | INTEGER   | Rating of the opponent for this game                                                       |
| my_first_move                     | VARCHAR   | First move made by 'mposada97'                                                             |
| opponent_first_move               | VARCHAR   | First move made by the opponent                                                            |
| my_remaining_time_seconds         | DOUBLE    | Remaining time for 'mposada97' at the end of the game, in seconds                          |
| opponent_remaining_time_seconds   | DOUBLE    | Remaining time for the opponent at the end of the game, in seconds                         |
| my_result                         | VARCHAR   | Result of the game for 'mposada97' (win, loss, draw)                                       |
| opponent_result                   | VARCHAR   | Result of the game for the opponent (win, loss, draw)                                      |
| number_of_rounds                  | INTEGER   | Number of rounds played in the game                                                        |
| termination                       | VARCHAR   | Reason for game termination (e.g., checkmate, time forfeit, resignation)                   |
| opening                           | VARCHAR   | Name of the chess opening used in the game (extracted from the full opening classification)|

Notes:
- This model is materialized as an incremental table.
- The unique key for this model is 'uuid'.
- The table is partitioned by 'year' and 'month'.
- The model filters for new data based on the 'end_time' column when run incrementally.

### agg_rapid_chess_daily

| Column Name  | Data Type      | Description                                                   |
|--------------|----------------|---------------------------------------------------------------|
| year         | INTEGER        | Year of the chess games                                       |
| month        | INTEGER        | Month of the chess games                                      |
| game_date    | DATE           | Date of the chess games                                       |
| average_elo  | DOUBLE         | Average ELO rating for the day                                |
| total_games  | INTEGER        | Total number of rapid chess games played on this date         |
| total_wins   | INTEGER        | Total number of games won on this date                        |
| total_losses | INTEGER        | Total number of games lost on this date                       |
| total_draws  | INTEGER        | Total number of games drawn on this date                      |
| game_details | ARRAY          | Array of detailed information for each game (see sub-table)   |

game_details Array Structure:
| Field Name                  | Data Type | Description                                         |
|-----------------------------|-----------|-----------------------------------------------------|
| game_uuid                   | VARCHAR   | Unique identifier for the game                      |
| game_date                   | DATE      | Date of the game                                    |
| my_rating                   | DOUBLE    | Player's rating for the game                        |
| opponent_rating             | DOUBLE    | Opponent's rating for the game                      |
| my_result                   | VARCHAR   | Result of the game for the player                   |
| opponent_result             | VARCHAR   | Result of the game for the opponent                 |
| number_of_rounds            | DOUBLE    | Number of rounds played in the game                 |
| my_remaining_time_seconds   | DOUBLE    | Player's remaining time at the end of the game      |
| opening                     | VARCHAR   | Chess opening used in the game                      |

Notes:
- This model is materialized as an incremental table.
- The unique key for this model is 'game_date'.
- The incremental strategy is set to 'merge'.
- The table is partitioned by 'year' and 'month'.
- The model only includes rapid chess games.
- When run incrementally, it only processes data from the latest game_date in the existing table onwards.

### agg_rapid_chess_monthly_openings

| Column Name   | Data Type      | Description                                                   |
|---------------|----------------|---------------------------------------------------------------|
| year          | INTEGER        | Year of the chess games                                       |
| month         | INTEGER        | Month of the chess games                                      |
| pieces_color  | VARCHAR        | Color of pieces played ('white', 'black', or 'overall')       |
| opening       | VARCHAR        | Chess opening used or 'overall' for aggregated data           |
| average_elo   | DOUBLE         | Average ELO rating, only for 'overall' pieces_color and opening |
| total_games   | INTEGER        | Total number of games played                                  |
| total_wins    | INTEGER        | Total number of games won                                     |
| total_losses  | INTEGER        | Total number of games lost                                    |
| total_draws   | INTEGER        | Total number of games drawn                                   |
| win_ratio     | DECIMAL(10, 2) | Ratio of games won to total games played                      |

Notes:
- This model is materialized as a table (not incremental).
- The table is partitioned by 'year' and 'month'.
- The model only includes rapid chess games.
- The data is aggregated using GROUPING SETS to provide various levels of aggregation:
  - By year, month, piece color, and opening
  - By year, month, and piece color
  - By year, piece color, and opening
  - By year and month
  - By year and piece color
  - By year only
- The average_elo is only populated for rows where both opening and pieces_color are 'overall'.
- The win_ratio is calculated as (total_wins / total_games) and rounded to two decimal places.

# The Pipelines:

