# Personal Performance Data Pipelines

The idea of this project was to build data pipelines around my personal data to help me track where I'm at in different aspects of my life and find areas of improvement. The first step was this diagram which I used to decide which aspects of my life I am interested in tracking and which have readily available data that I can consume through API's.

![image](https://github.com/user-attachments/assets/40ff19d1-5047-449e-9882-c4e1bb553c5a)

After completing the diagram I decided to leverage IoT technology with my Oura ring to get my health data that includes readiness, sleep, resilience and heart rate, I also have a Macro tracking app I use for my diet, I considered including it here but they dont have an API, there is a data dump but since it envolves manual work to update the data I decided not to include it in the project. I also decided to build a pipeline around my newest hobbie, which is chess, I have been playing games in chess.com, they have a public API that I can consume to get my games.

# Scope
- Create a daily batch pipeline for my health data (from my oura ring).
- Create a monthly batch pipeline for my chess games (from chess.com public API).
  - API's will be consumed using spark, dbt will be used for further modeling.
- Create data visualizations to give me more visibility to my health trends.
- Create data visualizations to track my ELO, and analyze which are my strengths and weaknesses when it comes to chess openings and pieces colors (for example: do I play better with white or black pieces? which openings should I study further to increase my win rate when those openings are played by my oponent, which openings should I keep playing myself?).

# Data Stack
This project is built on DataExpert.io's infrastructure (therefore, I only copied my scripts and DAGs to this repo, all of the AWS setup is left outside of this public repo to protect the bootcamp's infrastructure).

- Storage: AWS S3
- Metadata: Iceberg
- Query Engine: Trino
- Orchestrator: Airflow (Astro)
- Transformations, tests, and documentation: dbt
- Data Visualization: Superset
- Job Submission: AWS Glue
- API Consumption and Data Ingestion: Apache Spark (used for consuming APIs and ingesting data into S3)

This readme will be divided in two parts, chess and health (oura).

# Chess
## Data Modeling / Data Dictionary

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

### ranked_best_openings

| Column Name  | Data Type      | Description                                                   |
|--------------|----------------|---------------------------------------------------------------|
| pieces_color | VARCHAR        | Color of pieces played ('white' or 'black')                   |
| opening      | VARCHAR        | Chess opening name                                            |
| win_ratio    | DECIMAL(10, 2) | Ratio of games won to total games played for this opening     |
| rank         | INTEGER        | Rank of the opening based on win_ratio (descending order)     |

Notes:
- This model is materialized as a table.
- It selects the top 5 best performing openings for each piece color.
- Only includes openings that have been played in at least 1% of all rapid games.
- Excludes 'overall' aggregations for both pieces_color and opening.
- The ranking is based on the win_ratio in descending order (highest win_ratio ranked 1).

### ranked_worst_openings

| Column Name  | Data Type      | Description                                                   |
|--------------|----------------|---------------------------------------------------------------|
| pieces_color | VARCHAR        | Color of pieces played ('white' or 'black')                   |
| opening      | VARCHAR        | Chess opening name                                            |
| win_ratio    | DECIMAL(10, 2) | Ratio of games won to total games played for this opening     |
| rank         | INTEGER        | Rank of the opening based on win_ratio (ascending order)      |

Notes:
- This model is materialized as a table.
- It selects the top 5 worst performing openings for each piece color.
- Only includes openings that have been played in at least 1% of all rapid games.
- Excludes 'overall' aggregations for both pieces_color and opening.
- The ranking is based on the win_ratio in ascending order (lowest win_ratio ranked 1).

## The Pipeline:
You can find the code in the folders of this repository. Here is a picture of the Airflow DAG with run history, this DAG runs monthly:

![Screenshot 2024-09-28 171718](https://github.com/user-attachments/assets/15814239-3c76-4747-8f84-de5526a3f95d)

## Tests

I Included data tests and unit tests to ensure data quality. I leveraged dbt and yaml files to create these. Here is an example of the yaml file for my fct_chess_games table:

```yaml
version: 2

models:
  - name: fct_chess_games
    description: "This is a fact table for all my rapid chess games."
    columns:
      - name: uuid
        description: "Unique identifier for each game."
        data_tests:
          - not_null
          - unique

      - name: year
        description: "The year the game was played, used to partition the table."
        data_tests:
          - not_null

      - name: month
        description: "The month the game was played, used to partition the table."
        data_tests:
          - not_null

      - name: game_date
        description: "The date the game was played."
        data_tests:
          - not_null

      - name: end_time
        description: "A date time corresponding to the time that the game ended at."
        data_tests:
          - not_null

      - name: time_class
        description: "Game time class, its one of these values: rapid, bullet, blitz or daily"
        data_tests:
          - not_null

      - name: time_control
        description: "A string with the number of seconds in the time control of the game, for a 10 minute game its 600, if 3 seconds are added for every move its 600+3."
        data_tests:
          - not_null

      - name: my_color
        description: "The color of my pieces in the game."
        data_tests:
          - not_null

      - name: my_rating
        description: "My ELO rating before each game (at the start of the game)."
        data_tests:
          - not_null

      - name: opponent_username
        description: "Opponents username in a string"
        data_tests:
          - not_null

      - name: opponent_rating
        description: "Opponents rating at the start of the game."
        data_tests:
          - not_null

      - name: my_first_move
        description: "My first move of the game."

      - name: opponent_first_move
        description: "Opponents first move of the game."

      - name: my_remaining_time_seconds
        description: "Remaining time in seconds in my clock when the game ends."

      - name: opponent_remaining_time_seconds
        description: "Remaining time in seconds in my opponents clock when the game ends."

      - name: my_result
        description: "my result of the game for example: win, draw, abandoned, checkmated among others."

      - name: opponent_result
        description: "opponents result of the game for example: win, draw, abandoned, checkmated among others."

      - name: number_of_rounds
        description: "the number of rounds played as an integer."

      - name: termination
        description: "The result of the game, text describing who won and how."

      - name: opening
        description: "the opening played, for example The Queens Gambit."

unit_tests:
  - name: test_fct_chess_games
    model: fct_chess_games
    overrides:
      macros:
        # unit test this model in "incremental" mode
        is_incremental: true 
    given:
      - input: source('mposada', 'chess_games')
        format: sql
        rows: |
          select 'c1863db2-3756-11ef-9c58-ac951d01000f' as uuid, 2024 as year, 5 as month, cast('2024-05-01' as date) as game_date, TIMESTAMP '2024-05-01 03:17:28.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'Gato-buho' as white_username, 'mposada97' as black_username, 'Nf3' as white_first_move, 'c6' as black_first_move, 54 as number_of_rounds, '0:04:08.5' as white_remaining_time, '0:04:03.3' as black_remaining_time, '0-1' as result, 'win' as black_result, 'checkmated' as white_result, 'mposada97 won by checkmate' as termination, 'https://www.chess.com/openings/Caro-Kann-Defense-2.Nf3' as opening, 594 as white_rating, 597 as black_rating union all
          select '7a059341-3758-11ef-bfd0-e7ea6f01000f' as uuid, 2024 as year, 7 as month, cast('2024-07-14' as date) as game_date, TIMESTAMP '2024-07-14 03:36:01.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'chiheb_md' as white_username, 'mposada97' as black_username, 'e4' as white_first_move, 'e5' as black_first_move, 55 as number_of_rounds, '0:01:52.7' as white_remaining_time, '0:00:17.6' as black_remaining_time, '1-0' as result, 'checkmated' as black_result, 'win' as white_result, 'chiheb_md won by checkmate' as termination, 'https://www.chess.com/openings/Scotch-Game' as opening, 587 as white_rating, 589 as black_rating
      - input: this
        format: sql
        rows: |
              select 'c1863db2-3756-11ef-9c58-ac951d01000f' as uuid, 2024 as year, 5 as month, cast('2024-05-14' as date) as game_date, TIMESTAMP '2024-05-01 03:17:28.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'black' as my_color, 597 as my_rating, 'Gato-buho' as opponent_username, 594 as opponent_rating, 'c6' as my_first_move, 'Nf3' as opponent_first_move, cast(243.3 as double) as my_remaining_time_seconds, cast(248.5 as double) as opponent_remaining_time_seconds, 'win' as my_result, 'checkmated' as opponent_result, 54 as number_of_rounds, 'mposada97 won by checkmate' as termination, 'Caro-Kann-Defense-2.Nf3' as opening union all
              select '1c6e75e1-59d2-11ef-8db4-ce194301000f' as uuid, 2024 as year, 6 as month, cast('2024-06-14' as date) as game_date, TIMESTAMP '2024-06-14 00:23:41.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'white' as my_color, 608 as my_rating, 'StewieBaker' as opponent_username, 629 as opponent_rating, 'e4' as my_first_move, 'Nc6' as opponent_first_move, cast(284.7 as double) as my_remaining_time_seconds, cast(364.2 as double) as opponent_remaining_time_seconds, 'checkmated' as my_result, 'win' as opponent_result, 41 as number_of_rounds, 'StewieBaker won by checkmate' as termination, 'Nimzowitsch-Defense-Declined-2...d5' as opening union all
              select '2fcb7b1c-59e2-11ef-8db4-ce194301000f' as uuid, 2024 as year, 7 as month, cast('2024-07-14' as date) as game_date, TIMESTAMP '2024-07-14 02:18:28.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'black' as my_color, 600 as my_rating, 'Namalski' as opponent_username, 608 as opponent_rating, 'e5' as my_first_move, 'e4' as opponent_first_move, cast(341.7 as double) as my_remaining_time_seconds, cast(325.0 as double) as opponent_remaining_time_seconds, 'abandoned' as my_result, 'win' as opponent_result, 28 as number_of_rounds, 'Namalski won - game abandoned' as termination, 'Ponziani-Opening' as opening
    expect:
      format: sql
      rows: |
          select '7a059341-3758-11ef-bfd0-e7ea6f01000f' as uuid, 2024 as year, 7 as month, cast('2024-07-14' as date) as game_date, TIMESTAMP '2024-07-14 03:36:01.000000+00:00' as end_time, 'rapid' as time_class, '600' as time_control, 'black' as my_color, 589 as my_rating, 'chiheb_md' as opponent_username, 587 as opponent_rating, 'e5' as my_first_move, 'e4' as opponent_first_move, cast(17.6 as double) as my_remaining_time_seconds, cast(112.7 as double) as opponent_remaining_time_seconds, 'checkmated' as my_result, 'win' as opponent_result, 55 as number_of_rounds, 'chiheb_md won by checkmate' as termination, 'Scotch-Game' as opening
```
As you can see these yaml file especifies tests for individual columns such as not null, unique tests, and allowed values test. But it also includes unit tests which would test that your sql query will work as expected, it works for both incremental and full refresh models.

NOTE: For incremental models, the expected section will be the data coming into the table, it is not the data of the table after the incoming data arrives.

To run these tests I can write dbt test --select fct_chess_games in the terminal (in the directory of my dbt project). You can also include this as a step in the DAG, in this case I didnt do that because my initial understanding was that tests were run when the model was run, which is not the case, I will talk more about this in the Learnings and next steps section. Here is a screenshot of the tests for my fct_chess_games table, all passed:
![image](https://github.com/user-attachments/assets/c03e8050-4b54-4fa1-a6d1-d93c9289298e)


## Documentation
For documentation I used dbt. By setting up references in the sql files, and adding descriptions to models and columns in yaml files, dbt will be able to generate documentation, yuou can access it by typing dbt docs generate and the dbt docs serve in the terminal. This will launch a tab in your browser where you can see information about your models such as column types, description, dependencies and a lineage graph. For example:
![image](https://github.com/user-attachments/assets/ba5a8332-1292-4cfb-aa9c-bee5df65a768)


## Data Visualization
![Screenshot 2024-09-28 183752](https://github.com/user-attachments/assets/735456e0-48ea-4cfb-a82c-ec9c744ac53d)

This dashboard captures my chess journey, offering clear insights into my performance and areas for improvement. From the visualizations, I can track my ELO rating progression over time, observe my current standing, and identify which color pieces I perform better with (as expected, I tend to perform stronger with the white pieces). Additionally, the dashboard highlights my top-performing and weakest openings. For instance, I’ve had great success with the Scandinavian Defense, boasting an 80% win rate, so it’s a strategy worth continuing. On the other hand, the King's Pawn Opening and its variations appear as some of my worst-performing openings, signaling that I should reconsider using them when possible but that I should also study them  further when my opponent plays the opening.

## Learnings and Next Steps



