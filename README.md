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
### agg
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

# The Pipelines:

