from airflow import DAG
from datetime import datetime
from os.path import join
from operators.twitter_operator import TwitterOperator


start_date = datetime.now()

with DAG(dag_id='twitter', start_date=start_date) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_alura_online",
        query="AluraOnline",
        file_path=join(
            '/Users/raphael/Galpao/data_engineer/data_pipeline/datalake',
            'twitter_alura_online',
            'extract_date_{{ ds }}',
            'alura_online_{{ ds_nodash }}.json'
        )
    )