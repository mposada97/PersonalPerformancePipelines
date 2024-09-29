import sys
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, year, month, max, from_utc_timestamp
import requests

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

today = datetime.today()
start_date = today - timedelta(days=7)

# Format the dates in 'YYYY-MM-DD' format
start_date = start_date.strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d') #overlapping dates to make sure no missing data, use dbt to incrementally increase

# API endpoint and parameters
url = 'https://api.ouraring.com/v2/usercollection/daily_readiness' 
params = {
    'start_date': start_date,
    'end_date': end_date
}
oura_api_key = OURA_API # SET IN TERMINAL
headers = { 
  'Authorization': f'Bearer {oura_api_key}' 
}
response = requests.request('GET', url, headers=headers, params=params)
readiness = response.json()['data']
readiness_df = pd.json_normalize(readiness)
readiness_df.columns = readiness_df.columns.str.replace('contributors.', '')
readiness_df = spark.createDataFrame(readiness_df)

output_table = 'mposada.readiness'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        id STRING,
        year INTEGER,
        month INTEGER,
        day DATE,
        score DECIMAL,
        temperature_deviation DECIMAL,
        temperature_trend_deviation DECIMAL,
        timestamp TIMESTAMP,
        activity_balance DECIMAL,
        body_temperature DECIMAL,
        hrv_balance DECIMAL,
        previous_day_activity DECIMAL,
        previous_night_sleep_score DECIMAL,
        recovery_index DECIMAL,
        resting_heart_rate DECIMAL,
        sleep_balance DECIMAL
        )
        USING iceberg   
        PARTITIONED BY (year, month)     
        """
spark.sql(query)
#this is new
existing_data_df = spark.table(output_table)
if existing_data_df.count() > 0:
    latest_date = existing_data_df.select(max("day")).collect()[0][0]
else:
    latest_date = None

# Filter new data to include only records newer than the latest date
if latest_date:
    readiness_df = readiness_df.filter(col("day") > lit(latest_date))
# until here
readiness_df = readiness_df.select(
    col("id"),
    col("day").cast("date").alias("day"),
    month(col("day")).alias("month"),
    year(col("day")).alias("year"),
    col("score"),
    col("temperature_deviation"),
    col("temperature_trend_deviation"),
    col("timestamp").cast("timestamp").alias("timestamp"),
    col("activity_balance"),
    col("body_temperature"),
    col("hrv_balance"),
    col("previous_day_activity"),
    col("previous_night").alias("previous_night_sleep_score"),
    col("recovery_index"),
    col("resting_heart_rate"),
    col("sleep_balance")
)
readiness_df.writeTo(output_table).using("iceberg").append()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
