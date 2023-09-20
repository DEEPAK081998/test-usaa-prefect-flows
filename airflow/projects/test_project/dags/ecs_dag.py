import ast
import os
from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsOperator

file_name = os.path.splitext(os.path.basename(__file__))[0]


def execute_ecs_run(**kwargs) -> None:
    """
    Task to execute ecs run command
    :param kwargs (dict): dictionary of keyword arguments
    """
    project_dir = kwargs['project_dir']
    conn_id = kwargs['conn_id']
    cluster_name = kwargs['cluster_name']
    container_name = kwargs['container_name']
    task_arn = kwargs['task_arn']
    run_command = ast.literal_eval(kwargs['run_command'])
    conn = BaseHook.get_connection(conn_id)
    dbt_run = EcsOperator(
        task_id='ecs_run_task',
        dag=dag,
        cluster=cluster_name,
        task_definition=task_arn,
        launch_type='FARGATE',
        overrides={
            'containerOverrides': [
                {
                    'name': container_name,
                    'environment': [
                        {'name': 'DBT_PROJECT_PATH', 'value': project_dir},
                        {'name': 'DB_NAME', 'value': conn.conn_id},
                        {'name': 'DB_HOST', 'value': conn.host},
                        {'name': 'DB_PASS', 'value': conn.password},
                        {'name': 'DB_PORT', 'value': f'{conn.port}'},
                        {'name': 'DB_SCHEMA', 'value': conn.schema},
                        {'name': 'DB_TYPE', 'value': conn.conn_type},
                        {'name': 'DB_USER', 'value': conn.login},
                        {'name': 'DB_THREADS', 'value': '2'},
                    ],
                    'command': run_command,
                },
            ],
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-a3b29de8', 'subnet-7f610822'],
            },
        },
        tags={
            'Owner': 'vrathore',
            'Project': 'mdp',
            'Environment': 'sandbox',
        }
    )
    dbt_run.execute(dict())


with DAG(
        file_name,
        description='A simple ecs run DAG',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['test_project'],
        params={
            'dbt_project_path': Param(
                's3://mdp-sandbox-bucket/dbt/projects/re_reporting_dw_fix.zip',
                description='S3 Path of the zipped dbt project directory',
                type='string'
            ),
            'dbt_conn_id': Param('dbt_test2', description='DBT connection object ID on airflow', type='string'),
            'run_command': Param(
                ['run', '--select', 'brokerage_assignments'],
                description='dbt command that needed to run',
                type='array'
            ),
            'cluster_name': Param(
                'vrathore-test-stack-sandbox-ecs-cluster',
                description='AWS ECS cluster name',
                type='string'
            ),
            'container_name': Param(
                'vrathore-test-stack-sandbox-workers',
                description='ECS container name',
                type='string'
            ),
            'task_arn': Param(
                'arn:aws:ecs:us-east-1:429533234373:task-definition/vrathore-test-stack-sandbox-workers-task-definition',
                description='task definition arn',
                type='string'
            ),
        }
) as dag:
    PythonOperator(
        task_id='dbt_run_task',
        python_callable=execute_ecs_run,
        op_kwargs={
            'run_command': '{{ params.run_command}}',
            'project_dir': '{{params.dbt_project_path}}',
            'conn_id': '{{ params.dbt_conn_id }}',
            'cluster_name': '{{ params.cluster_name }}',
            'container_name': '{{ params.container_name }}',
            'task_arn': '{{ params.task_arn }}',
        }
    )
