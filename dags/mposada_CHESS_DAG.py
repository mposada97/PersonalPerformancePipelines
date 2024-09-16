from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job
from airflow.operators.empty import EmptyOperator
from include.eczachly.aws_secret_manager import get_secret
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query
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


# Define paths to the DBT project and virtual environment
airflow_home = os.environ['AIRFLOW_HOME']
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'
PATH_TO_DBT_VARS = f'{airflow_home}/dbt_project/dbt.env'
ENTRYPOINT_CMD = f"source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}"


@dag("mposada_CHESS_DAG",
     description="Chessdotcom API consumption and data modeling/processing",
     default_args={
         "owner": "Mateo Posada",
         "start_date": datetime(2024, 5, 1),
         "retries": 1,
     },
     max_active_runs=1,
     schedule_interval="@monthly",
     catchup=False,
     tags=["pyspark", "glue", "chess", "mposada"],
     template_searchpath='include/eczachly')
def mposada_CHESS_DAG():
    chessdotcom_api_consumption = PythonOperator(
        task_id="chessdotcom_api",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "mposada_CHESS_API",
            "script_path": "include/eczachly/scripts/chess_games_api.py",
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Chess games API data ingestion",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'mposada.chess_games'
            },
        },
    )
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{ENTRYPOINT_CMD} && dbt deps',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    dbt_run_fct_chess_games = BashOperator(
        task_id='dbt_run_fct_chess_games',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s fct_chess_games',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    dbt_run_agg_rapid_chess_daily = BashOperator(
        task_id='dbt_run_agg_rapid_chess_daily',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s agg_rapid_chess_daily',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    dbt_run_agg_rapid_chess_monthly_openings = BashOperator(
        task_id='dbt_run_agg_rapid_chess_monthly_openings',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s agg_rapid_chess_monthly_openings',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    (start >> chessdotcom_api_consumption >> dbt_deps >> dbt_run_fct_chess_games >> 
    [dbt_run_agg_rapid_chess_daily, 
     dbt_run_agg_rapid_chess_monthly_openings
    ] >> end)


mposada_CHESS_DAG()