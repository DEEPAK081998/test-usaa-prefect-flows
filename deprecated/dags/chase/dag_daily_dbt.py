import ast
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow_plugins import get_airflow_dag_default_args


def execute_dbt_run(**kwargs):
    """
    Task to execute dbt run command
    :param kwargs (dict): dictionary of keyword arguments
    """
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    project_dir = kwargs['project_dir']
    target = kwargs['target']

    dbt_extra_kwargs_map = ast.literal_eval(dbt_extra_kwargs)
    dbt_run = DbtRunOperator(
        task_id="dbt_run_example",
        project_dir=project_dir,
        target=target,
        full_refresh=True,
        threads=10,
        #exclude=["tag:mlo_invites+"],
        **dbt_extra_kwargs_map
    )
    dbt_run.execute(dict())


file_name = os.path.splitext(os.path.basename(__file__))[0]

with DAG(
        file_name,
        description='Runs dbt DBT task',
        schedule_interval=Variable.get(f'{file_name}_schedule', default_var=None),
        start_date=datetime(2022, 1, 1),
        default_args=get_airflow_dag_default_args(),
        catchup=False,
        tags=['dbt', 'chase'],
        params={
            'connection_id': Param(
                '3a345a5f-b273-4efd-abf2-42454956590e', description='Connection ID on airbyte', type='string',
            ),
            'dbt_project_path': Param(
                's3://mdp-prod-chase-bucket/dbt/projects/chase.zip',
                description='S3 Path of the zipped dbt project directory',
                type='string'
            ),
            'dbt_conn_id': Param('warehouse_db', description='DBT connection object ID on airflow', type='string'),
            'dbt_extra_kwargs': Param({}, description='Extra kwargs to include while running DBT run', type='object'),
        },
) as dag:
    logging.info(f'get_airflow_dag_default_args(): {get_airflow_dag_default_args()}')
    dbt_run_task = PythonOperator(
        task_id='dbt_run_task',
        python_callable=execute_dbt_run,
        op_kwargs={
            'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
            'project_dir': '{{params.dbt_project_path}}',
            'target': '{{ params.dbt_conn_id }}',
        }
    )


    dbt_run_task