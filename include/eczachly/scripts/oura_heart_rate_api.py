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
url = 'https://api.ouraring.com/v2/usercollection/heartrate'
params = {
    'start_date': start_date,
    'end_date': end_date
}
oura_api_key = 'LIZMF5BT2M22DF3CTRJSHPA4WCTCVZFE'
headers = { 
  'Authorization': f'Bearer {oura_api_key}' 
}
response = requests.request('GET', url, headers=headers, params=params)
heart_rate = response.json()['data']
heart_rate_df = pd.json_normalize(heart_rate)
heart_rate_df.columns = heart_rate_df.columns.str.replace('contributors.', '')
heart_rate_df = spark.createDataFrame(heart_rate_df)

output_table = 'mposada.heart_rate'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        bpm DECIMAL,
        source STRING,
        timestamp TIMESTAMP,
        utc_day DATE,
        local_day DATE
        )
        USING iceberg   
        PARTITIONED BY (local_day)    
        """
spark.sql(query)
#this is new
existing_data_df = spark.table(output_table)
if existing_data_df.count() > 0:
    latest_date = existing_data_df.select(max("timestamp")).collect()[0][0]
else:
    latest_date = None

# Filter new data to include only records newer than the latest date
if latest_date:
    heart_rate_df = heart_rate_df.filter(col("timestamp").cast("timestamp") > lit(latest_date))
# until here
heart_rate_df = heart_rate_df.select(
    col("bpm"),
    col("source"),
    col("timestamp").cast("timestamp"),
    col("timestamp").cast("date").alias("utc_day"),
    from_utc_timestamp(col("timestamp"), "America/Chicago").cast("date").alias("local_day")  # Extract local date for partitioning
)
heart_rate_df.writeTo(output_table).using("iceberg").append()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
