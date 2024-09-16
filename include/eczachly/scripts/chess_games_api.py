import sys
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col, year, month, max, unix_timestamp, from_unixtime, to_date, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DoubleType
# Register the UDF
import requests
import re

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Replace 'chess_player_username' with the actual Chess.com username
username = 'mposada97'
url = f'https://api.chess.com/pub/player/{username}/games/archives'
headers = {
    'User-Agent': 'I am trying to get my chess.com data for a data engineering portfolio project. Contact me at mateo.posada1997@gmail.com'
}

# Get the list of archive URLs
response = requests.get(url, headers=headers)
response.raise_for_status()  # Raises an HTTPError for bad responses
archives = response.json().get('archives', [])

all_games = []

# Iterate through the archive URLs and retrieve the games data
for archive_url in archives:
    try:
        archive_response = requests.get(archive_url, headers=headers)
        archive_response.raise_for_status()  # Raises an HTTPError for bad responses
        games_data = archive_response.json().get('games', [])
        all_games.extend(games_data)
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

def extract_game_details(pgn):
    try:
        metadata, moves = pgn.split('\n\n', 1)
        metadata_lines = metadata.split('\n')

        white_first_move = None
        black_first_move = None
        white_move_match = re.search(r'1\. ([^\s]+)', moves)
        if white_move_match:
            white_first_move = white_move_match.group(1)
        
        black_move_match = re.search(r'1\. .* 1\.\.\. ([^\s]+)', moves)
        if black_move_match:
            black_first_move = black_move_match.group(1)

        round_matches = re.findall(r'\d+\.\s', moves)
        number_of_rounds = len(round_matches)

        # Extract remaining time for white and black
        white_times = re.findall(r'\d+\.\s[^\s]+\s\{\[%clk ([0-9:]+(?:\.[0-9]+)?)\]\}', moves)
        white_remaining_time = white_times[-1] if white_times else None

        black_times = re.findall(r'\d+\.\.\.\s[^\s]+\s\{\[%clk ([0-9:]+(?:\.[0-9]+)?)\]\}', moves)
        black_remaining_time = black_times[-1] if black_times else None
        
        def extract_metadata_value(tag):
            line = next((line for line in metadata_lines if line.startswith(f'[{tag} ')), None)
            return line.split('"')[1] if line else None
        
        # Extract only the date field
        event = extract_metadata_value("Event")
        site = extract_metadata_value("Site")
        date = extract_metadata_value("Date")
        round = extract_metadata_value("Round")
        result = extract_metadata_value("Result")
        time_control = extract_metadata_value("TimeControl")
        termination = extract_metadata_value("Termination")
        eco = extract_metadata_value("ECO")
        opening = extract_metadata_value("ECOUrl")
        
        return Row(event=event, site=site, date=date, round=round, result=result, 
                   time_control=time_control, termination=termination, eco=eco, opening=opening,
                   white_first_move=white_first_move, black_first_move=black_first_move, 
                   number_of_rounds=number_of_rounds, 
                   white_remaining_time=white_remaining_time, black_remaining_time=black_remaining_time)
    except Exception as e:
        print(f"Error parsing PGN: {e}")
        return Row(event=None, site=None, date=None, round=None, result=None, 
                   time_control=None, termination=None, eco=None, opening=None,
                   white_first_move=None, black_first_move=None, 
                   number_of_rounds=None, 
                   white_remaining_time=None, black_remaining_time=None)



schema = StructType([
    StructField("event", StringType(), True),
    StructField("site", StringType(), True),
    StructField("date", StringType(), True),
    StructField("round", StringType(), True),
    StructField("result", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("termination", StringType(), True),
    StructField("eco", StringType(), True),
    StructField("opening", StringType(), True),
    StructField("white_first_move", StringType(), True),
    StructField("black_first_move", StringType(), True),
    StructField("number_of_rounds", IntegerType(), True),
    StructField("white_remaining_time", StringType(), True),
    StructField("black_remaining_time", StringType(), True)
])

extract_game_details_udf = udf(extract_game_details, schema)

# Assuming all_games is the list of dictionaries
# Convert JSON to Spark DataFrame
df = spark.createDataFrame(pd.json_normalize(all_games))

# Rename columns with periods
df = df.withColumnRenamed("white.username", "white_username") \
       .withColumnRenamed("black.username", "black_username") \
       .withColumnRenamed("white.rating", "white_rating") \
       .withColumnRenamed("black.rating", "black_rating") \
       .withColumnRenamed("white.result", "white_result") \
       .withColumnRenamed("black.result", "black_result")

# Add columns for extracted data using the UDF
df_with_details = df.withColumn("game_details", extract_game_details_udf(col("pgn")))

df_with_details = df_with_details.withColumn("game_date", to_date(col("game_details.date"), 'yyyy.MM.dd')) \
                                 .withColumn("year", year(col("game_date"))) \
                                 .withColumn("month", month(col("game_date")))

# Select and rename columns
df_final = df_with_details.select(
    col("uuid"),
    col("year"),
    col("month"),
    col("game_date"),
    from_unixtime(col("end_time")).cast("timestamp").alias("end_time"),
    col("time_class"),
    col("time_control"),
    col("white_username"),
    col("black_username"),
    col("game_details.white_first_move"),
    col("game_details.black_first_move"),
    col("game_details.number_of_rounds"),
    col("game_details.white_remaining_time"),
    col("game_details.black_remaining_time"),
    col("game_details.result"),
    col("black_result"),
    col("white_result"),
    col("game_details.termination"),
    col("game_details.opening"),
    col("white_rating"),
    col("black_rating")
)

# Define the schema for the output table
schema = StructType([
    StructField("uuid", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("game_date", DateType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("time_class", StringType(), True),
    StructField("time_control", StringType(), True),
    StructField("white_username", StringType(), True),
    StructField("black_username", StringType(), True),
    StructField("white_first_move", StringType(), True),
    StructField("black_first_move", StringType(), True),
    StructField("number_of_rounds", IntegerType(), True),
    StructField("white_remaining_time", StringType(), True),
    StructField("black_remaining_time", StringType(), True),
    StructField("result", StringType(), True),
    StructField("black_result", StringType(), True),
    StructField("white_result", StringType(), True),
    StructField("termination", StringType(), True),
    StructField("opening", StringType(), True),
    StructField("white_rating", IntegerType(), True),
    StructField("black_rating", IntegerType(), True)
])

# Create Spark DataFrame with explicit schema
game_details_df = spark.createDataFrame(df_final.rdd, schema)

# Create the output table if not exists
output_table = "mposada.chess_games"
query = f"""
CREATE TABLE IF NOT EXISTS {output_table} (
    uuid STRING,
    year INTEGER,
    month INTEGER,
    game_date DATE,
    end_time TIMESTAMP,
    time_class STRING,
    time_control STRING,
    white_username STRING,
    black_username STRING,
    white_first_move STRING,
    black_first_move STRING,
    number_of_rounds INTEGER,
    white_remaining_time STRING,
    black_remaining_time STRING,
    result STRING,
    black_result STRING,
    white_result STRING,
    termination STRING,
    opening STRING,
    white_rating INTEGER,
    black_rating INTEGER
) USING iceberg
PARTITIONED BY (year, month)
"""
spark.sql(query)

# Check if table has existing data
existing_data_df = spark.table(output_table)
if existing_data_df.count() > 0:
    latest_date = existing_data_df.select(max("end_time")).collect()[0][0]
else:
    latest_date = None

# Filter new data to include only records newer than the latest date
if latest_date:
    game_details_df = game_details_df.filter(col("end_time") > lit(latest_date))

# Write the data to the output table
game_details_df.writeTo(output_table).using("iceberg").append()

# Initialize and commit the Glue job
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)