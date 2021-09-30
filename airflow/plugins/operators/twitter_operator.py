from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from hooks.twitter_hook import TwitterHook
from datetime import datetime
import json


class TwitterOperator(BaseOperator):


    def __init__(
        self,
        query,
        conn_id=None,
        start_time=None,
        end_time=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time
        

    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time,
        )

        for data in hook.run():
            print(json.dumps(data, indent=4, sort_keys=True))


if __name__ == '__main__':
    now = datetime.now()

    with DAG(dag_id='TwitterTest', start_date=now) as dag:
        operator = TwitterOperator(
            query='AluraOnline',
            task_id='test_run'
        )

        task = TaskInstance(task=operator, execution_date=now)
        operator.execute(task.get_template_context())