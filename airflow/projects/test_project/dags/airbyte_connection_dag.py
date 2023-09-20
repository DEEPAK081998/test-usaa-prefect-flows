import os

from datetime import datetime

from airflow import DAG

from airflow.models.param import Param

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

file_name = os.path.splitext(os.path.basename(__file__))[0]

with DAG(
    file_name, # maintains uniqueness across DAGS
    description='A sample DAG that creates a connection to airbyte',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['airbyte', 'test_project'],
    params={
        'connection_id': Param('', description='Connection ID on airbyte', type='string'),
    },
) as dag:

    AirbyteTriggerSyncOperator(
        task_id='airbyte_conn_example',
        airbyte_conn_id='airbyte_connection',
        connection_id='{{ params.connection_id }}',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
