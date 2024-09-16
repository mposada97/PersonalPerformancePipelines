# Personal Performance Pipelines

The idea of this project was to build data pipelines around my personal data. The first step was this diagram which I used to decide which aspects of my life I am interested in tracking and which have readily available data that I can consume through API's.

![image](https://github.com/user-attachments/assets/40ff19d1-5047-449e-9882-c4e1bb553c5a)

After completing the diagram I decided to leverage IoT techonlogy with my Oura ring to get my health data that includes readiness, sleep, resilience and heart rate. I also decided to buuild a pipeline around my newest hobbie, which is chess, I have been playing games in chess.com, they have a public API that I can consume to get my games.

# Scope
- Create a daily batch pipeline for my health data (from my oura ring).
- Create a monthly batch pipeline for my chess games (from chess.com public API).
  - API's will be consumed using spark, dbt will be used for further modeling.
- Create data visualizations to give me more visibility to my health trends.
- Create data visualizations to track my ELO, and analyze which are my strengths and weaknesses when it comes to chess openings and pieces colors (for example: do I play better with white or black pieces? which openings should I study firther to increase my win rate when those openings are played by my oponent, which openings should I keep playing myself?).

# Data Stack
This project is built on DataExpert.io's infraestructure (therefore, I only copied my scripts and DAGs to this repo).
Storage: AWS S3
Metadata: Iceberg
Query Engine: Trino
Orchestrator: Airflow (Astro)
Transformations and tests: dbt
Data Visualization: 

