from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job
from airflow.operators.empty import EmptyOperator
from include.eczachly.aws_secret_manager import get_secret
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query
from airflow.operators.bash import BashOperator
import os

#s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
#tabular_credential = get_secret("TABULAR_CREDENTIAL")
#catalog_name = get_secret("CATALOG_NAME")  # "eczachly-academy-warehouse"
#aws_region = get_secret("AWS_GLUE_REGION")  # "us-west-2"
#aws_access_key_id = get_secret("DATAEXPERT_AWS_ACCESS_KEY_ID")
#aws_secret_access_key = get_secret("DATAEXPERT_AWS_SECRET_ACCESS_KEY")

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")

airflow_home = os.environ['AIRFLOW_HOME']
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'
PATH_TO_DBT_VARS = f'{airflow_home}/dbt_project/dbt.env'
ENTRYPOINT_CMD = f"source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}"


@dag("mposada_OURA_DAG",
     description="Oura Ring API consumption and data modeling/processing",
     default_args={
         "owner": "Mateo Posada",
         "start_date": datetime(2024, 5, 1),
         "retries": 1,
     },
     max_active_runs=1,
     schedule_interval="@daily",
     catchup=False,
     tags=["pyspark", "glue", "OURA_RING", "mposada"],
     template_searchpath='include/eczachly')
def mposada_OURA_DAG():
    sleep_api_consumption = PythonOperator(
        task_id="sleep_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_OURA_SLEEP_API",
            "script_path": "include/eczachly/scripts/oura_sleep_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Oura API sleep data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.sleep_data'
            },
        },
    )
    heart_rate_api_consumption = PythonOperator(
        task_id="heart_rate_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_OURA_HEART_RATE_API",
            "script_path": "include/eczachly/scripts/oura_heart_rate_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Oura API sleep data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.heart_rate'
            },
        },
    )
    readiness_api_consumption = PythonOperator(
        task_id="readiness_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_OURA_READINESS_API",
            "script_path": "include/eczachly/scripts/oura_readiness_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Oura API readiness data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.readiness'
            },
        },
    )
    resilience_api_consumption = PythonOperator(
        task_id="resilience_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_OURA_RESILIENCE_API",
            "script_path": "include/eczachly/scripts/oura_resilience_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Oura API resilience data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.resilience'
            },
        },
    )
    personal_info_api_consumption = PythonOperator(
        task_id="personal_info_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_OURA_Personal_info_API",
            "script_path": "include/eczachly/scripts/oura_personal_info_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Oura API personal data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.personal_info'
            },
        },
    )
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{ENTRYPOINT_CMD} && dbt deps',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    dbt_run_daily_stats = BashOperator(
        task_id='dbt_run_daily_stats',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s daily_health',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    dbt_run_monthly_stats = BashOperator(
        task_id='dbt_run_monthly_stats',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s agg_monthly_health',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    (start >> [ 
      sleep_api_consumption, 
      heart_rate_api_consumption,
      readiness_api_consumption,
      resilience_api_consumption,
      personal_info_api_consumption,
    ] >> dbt_deps >> dbt_run_daily_stats >> dbt_run_monthly_stats >> end)

mposada_OURA_DAG()