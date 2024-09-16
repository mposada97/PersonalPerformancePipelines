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
url = 'https://api.ouraring.com/v2/usercollection/daily_resilience'
params = {
    'start_date': start_date,
    'end_date': end_date
}
oura_api_key = 'LIZMF5BT2M22DF3CTRJSHPA4WCTCVZFE'
headers = { 
  'Authorization': f'Bearer {oura_api_key}' 
}
response = requests.request('GET', url, headers=headers, params=params)
resilience = response.json()['data']
resilience_df = pd.json_normalize(resilience)
resilience_df.columns = resilience_df.columns.str.replace('contributors.', '')
resilience_df = spark.createDataFrame(resilience_df)

output_table = 'mposada.resilience'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        id STRING,
        year INTEGER,
        month INTEGER,
        day DATE,
        level STRING,
        sleep_recovery DECIMAL,
        daytime_recovery DECIMAL,
        stress DECIMAL
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
    resilience_df = resilience_df.filter(col("day") > lit(latest_date))
# until here
resilience_df = resilience_df.select(
    col("id"),
    col("day").cast("date").alias("day"),
    month(col("day")).alias("month"),
    year(col("day")).alias("year"),
    col("level"),
    col("sleep_recovery"),
    col("daytime_recovery"),
    col("stress")
)
resilience_df.writeTo(output_table).using("iceberg").append()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
