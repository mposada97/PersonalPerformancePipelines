import sys
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, year, month, max
import requests
spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

today = datetime.today()
yesterday = today - timedelta(days=7)

# Format the dates in 'YYYY-MM-DD' format
start_date = yesterday.strftime('%Y-%m-%d')
end_date = today.strftime('%Y-%m-%d') #overlapping dates to make sure no missing data, use dbt to incrementally increase

# API endpoint and parameters
url = 'https://api.ouraring.com/v2/usercollection/daily_sleep'
params = {
    'start_date': start_date,
    'end_date': end_date
}
headers = { 
  'Authorization': f'Bearer {OURA_API}'
}
response = requests.request('GET', url, headers=headers, params=params)
sleep_data = response.json()['data']
sleep_data_df = pd.json_normalize(sleep_data)
sleep_data_df.columns = sleep_data_df.columns.str.replace('contributors.', '')
sleep_data_df = spark.createDataFrame(sleep_data_df)

output_table = 'mposada.sleep_data'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        id STRING,
        year INTEGER,
        month INTEGER,
        day DATE,
        score DECIMAL,
        timestamp TIMESTAMP,
        deep_sleep DECIMAL,
        efficiency DECIMAL,
        latency DECIMAL,
        rem_sleep DECIMAL,
        restfulness DECIMAL,
        timing DECIMAL,
        total_sleep DECIMAL
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
    sleep_data_df = sleep_data_df.filter(col("day") > lit(latest_date))
# until here
sleep_data_df = sleep_data_df.select(
    col("id"),
    col("day").cast("date").alias("day"),
    month(col("day")).alias("month"),
    year(col("day")).alias("year"),
    col("score"),
    col("timestamp").cast("timestamp").alias("timestamp"),
    col("deep_sleep"),
    col("efficiency"),
    col("latency"),
    col("rem_sleep"),
    col("restfulness"),
    col("timing"),
    col("total_sleep")
)
sleep_data_df.writeTo(output_table).using("iceberg").append()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
