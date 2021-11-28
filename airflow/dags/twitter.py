from airflow import DAG
from datetime import datetime
from os.path import join
from pathlib import Path
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

start_date = datetime.now()

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

BASE_FOLDER = join(
    str(Path("~/Galpao").expanduser()),
    'data_engineer/data_pipeline/datalake/{stage}/twitter_alura_online/{partition}'
)
PARTITION_FOLDER = 'extract_date={{ ds }}'
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id='twitter',
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    
    twitter_operator = TwitterOperator(
        task_id="twitter_alura_online",
        query="AluraOnline",
        file_path=join(
            BASE_FOLDER.format(stage='bronze', partition=PARTITION_FOLDER),
            'alura_online_{{ ds_nodash }}.json'
        ),
        start_time=(
            "{{"
            f"execution_date.strftime('{TIMESTAMP_FORMAT}')"
            "}}"
        ),
        end_time=(
            "{{"
            f"next_execution_date.strftime('{TIMESTAMP_FORMAT}')"
            "}}"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_alura_online",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/transformation.py",
        ),
        name="twitter_transformation",
        # application_args=[
        #     "--source", 
        #     "--path-dest",
        #     "--extract-date",
        # ]
    )

    twitter_operator >> twitter_transform