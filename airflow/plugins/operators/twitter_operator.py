from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from hooks.twitter_hook import TwitterHook
from datetime import datetime, timedelta
import json
from pathlib import Path
from os.path import join


class TwitterOperator(BaseOperator):

    template_fields = [
        'query',
        'file_path',
        'start_time',
        'end_time',
    ]


    def __init__(
        self,
        query,
        file_path,
        conn_id=None,
        start_time=None,
        end_time=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time


    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)


    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time,
        )

        self.create_parent_folder()

        with open(self.file_path, 'w') as output_file:
            for data in hook.run():
                json.dump(data, output_file, ensure_ascii=False)
                output_file.write("\n")



if __name__ == '__main__':
    start_date = datetime.now() - timedelta(days=1)

    with DAG(dag_id='TwitterTest', start_date=start_date) as dag:
        operator = TwitterOperator(
            query='AluraOnline',
            file_path=join(
                '/Users/raphael/Galpao/data_engineer/data_pipeline/datalake',
                'twitter_alura_online',
                'extract_date_{{ ds }}',
                'alura_online_{{ ds_nodash }}.json'
            ),
            task_id='test_run'
        )

        task = TaskInstance(task=operator, execution_date=start_date)
        task.run()
        