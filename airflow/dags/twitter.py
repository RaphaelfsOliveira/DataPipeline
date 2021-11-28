from airflow import DAG
from datetime import datetime
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


start_date = datetime.now()

with DAG(dag_id='twitter', start_date=start_date) as dag:
    
    twitter_operator = TwitterOperator(
        task_id="twitter_alura_online",
        query="AluraOnline",
        file_path=join(
            '/Users/raphael/Galpao/data_engineer/data_pipeline/datalake',
            'twitter_alura_online',
            'extract_date={{ ds }}',
            'alura_online_{{ ds_nodash }}.json'
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_alura_online",
        application="/Users/raphael/Galpao/data_engineer/data_pipeline/spark/transformation.py",
        name="twitter_transformation",
        # application_args=[
        #     "--source", 
        #     "--path-dest",
        #     "--extract-date",
        # ]
    )