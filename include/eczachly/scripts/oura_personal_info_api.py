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
url = 'https://api.ouraring.com/v2/usercollection/personal_info'
params = {
    'start_date': start_date,
    'end_date': end_date
}
oura_api_key = OURA_API # SET IN TERMINAL
headers = { 
  'Authorization': f'Bearer {oura_api_key}' 
}
response = requests.request('GET', url, headers=headers, params=params)
personal = response.json()
personal_df = pd.json_normalize(personal)
personal_df.columns = personal_df.columns.str.replace('contributors.', '')
personal_df = spark.createDataFrame(personal_df)

output_table = 'mposada.personal_info'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        age INTEGER,
        weight DECIMAL(10,2),
        height DECIMAL(10,2),
        biological_sex STRING
        )
        USING iceberg    
        """
spark.sql(query)

# until here
personal_df = personal_df.select(
    col("age"),
    col("weight").cast("decimal(10,2)").alias("weight"),
    col("height").cast("decimal(10,2)").alias("height"),
    col("biological_sex")
)
personal_df.writeTo(output_table).using("iceberg").overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
