import os

from datetime import datetime

from airflow import DAG

from airflow.models.param import Param

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from airflow_dbt_python.operators.dbt import DbtRunOperator

file_name = os.path.splitext(os.path.basename(__file__))[0]

with DAG(
    file_name,
    description='A sample DAG that runs a DBT task',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['dbt', 'test_project'],
    params={
        'connection_id': Param('', description='Connection ID on airbyte', type='string'),
        'dbt_conn_id': Param('', description='DBT connection object ID on airflow', type='string'),
        'dbt_project_path': Param('', description='S3 Path of the zipped dbt project directory', type='string'),
    },
) as dag:

    airbyte_connection_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_conn_example',
        airbyte_conn_id='airbyte_connection',
        connection_id='{{ params.connection_id }}',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_run = DbtRunOperator(
        task_id="dbt_run_example",
        project_dir='{{params.dbt_project_path}}',
        target='{{ params.dbt_conn_id }}',
        full_refresh=False,
    )

    # Sets dbt_run as a downstream task to airbyte_connection_sync hence, dbt_run will run
    # after airbyte_connection_sync
    airbyte_connection_sync >> dbt_run
