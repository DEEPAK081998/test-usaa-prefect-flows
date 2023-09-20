import os, ast

from datetime import datetime

from airflow import DAG

from airflow.models import Variable

from airflow.models.param import Param

from airflow.operators.python import PythonOperator

from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtTestOperator

from airflow.operators.bash import BashOperator



from airflow_plugins import get_airflow_dag_default_args


def execute_dbt_run(**kwargs):
    '''
    Task to execute dbt run command
    :param kwargs (dict): dictionary of keyword arguments
    '''
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    project_dir = kwargs['project_dir']
    target = kwargs['target']

    dbt_extra_kwargs_map = ast.literal_eval(dbt_extra_kwargs)
    dbt_run = DbtRunOperator(
        task_id="dbt_stellar_test",
        #project_dir='/usr/local/dbt/project_dir',
        project_dir= kwargs['project_dir'],
        target=target,
        threads=25,
        select=["brokerage_assignments"],
        #exclude=["tag:mlo_invites+"],
        #vars={"incremental_catch_up": "True"},
        full_refresh=False,
        **dbt_extra_kwargs_map
    )
    dbt_run.execute(dict())

def execute_dbt_test(**kwargs):
    '''
    Task to execute dbt run command
    :param kwargs (dict): dictionary of keyword arguments
    '''
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    #project_dir = kwargs['project_dir']
    target = kwargs['target']

    dbt_extra_kwargs_map = ast.literal_eval(dbt_extra_kwargs)
    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir='/usr/local/dbt/project_dir',
        target=target,
        #select=['+tag:raw','']
    )
    dbt_test.execute(dict())
file_name = 'dag_daily_dbt'

with DAG(
    file_name,
    description='Runs airbyte connections and then triggers a DBT task',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=get_airflow_dag_default_args(), # default args include task failure alert etc.
    tags=['dbt', 'airbyte', 're_reporting'],
    params={
        # TODO: Add this IN ENG-270
        # 'table_name': Param('', description='Dynamo DB table name', type='string'),
        # 'project_name': Param('', description='Dyname DB project name', type='string'),

        # TODO: Delete 'connection_id' param in ENG-270
        
        'dbt_project_path': Param(
            '/usr/local/dbt/re_reporting_dw_fix',
            description='S3 Path of the zipped dbt project directory',
            type='string'
        ),
        'dbt_conn_id': Param('dbt_test2', description='DBT connection object ID on airflow', type='string'),
        'dbt_extra_kwargs': Param({}, description='Extra kwargs to include while running DBT run', type='object'),
    },
) as dag:
    

    # TODO: Remove the code below in ENG-270
    #airbyte_connection_sync = AirbyteTriggerSyncOperator(
    #    task_id='airbyte_conn_example',
    #    airbyte_conn_id='airbyte_connection',
    #    connection_id='{{ params.connection_id }}',
    #    asynchronous=False,
    #    timeout=3600,
    #    wait_seconds=3
    #)

    dbt_run_task = PythonOperator(
        task_id='dbt_run_task',
        python_callable=execute_dbt_run,
        op_kwargs={
            'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
            'project_dir': '{{params.dbt_project_path}}',
            'target': '{{ params.dbt_conn_id }}',
        }
    )
    #dbt_test = PythonOperator(
    #    task_id='dbt_test_task',
    #    python_callable=execute_dbt_test,
    #    op_kwargs={
    #        'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
    #        'project_dir': '{{params.dbt_project_path}}',
    #        'target': '{{ params.dbt_conn_id }}',
    #    }
    #)

    #copy_credentials = BashOperator(
    #    task_id='print_hello_world',
    #    bash_command="echo 'hello world'",
    #)
    # Sets dbt_run as a downstream task to airbyte_connection_sync hence, dbt_run will run
    # after airbyte_connection_sync
    dbt_run_task 


