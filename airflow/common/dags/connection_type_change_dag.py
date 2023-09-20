import os
import logging
from airflow import DAG, settings, AirflowException
from datetime import datetime
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from airflow_plugins import get_airflow_dag_default_args

file_name = os.path.splitext(os.path.basename(__file__))[0]

def change_connection_type(conn_id: str, target_conn_type: str) -> None:
    '''
    Task to change airflow connection type
    :param conn_id: Connection id for which to change the type
    :param target_conn_type: Target connection type
    '''
    if conn_id and target_conn_type:
        logging.info(f'Connection id: {conn_id}, Target connection type: {target_conn_type}')
        conn = BaseHook.get_connection(conn_id=conn_id)
        conn.conn_type = target_conn_type
        session = settings.Session()
        session.add(conn)
        session.commit()
    else:
        raise AirflowException('Both conn_id and target_conn_type should be present')

with DAG(
    file_name,
    description='Change connection type on airflow',
    start_date=datetime(2022, 1, 1),
    schedule_interval=Variable.get(f'{file_name}_schedule', default_var=None),
    catchup=False,
    default_args=get_airflow_dag_default_args(),
    tags=['airflow'],
	params={
        'conn_id': Param('', description='Connection ID on airbyte', type='string',),
        'target_conn_type': Param('', description='Target connection type', type='string'),
    },
) as dag:
    PythonOperator(
        task_id='change_connection_type',
        python_callable=change_connection_type,
        op_kwargs={
            'conn_id': '{{ params.conn_id}}',
            'target_conn_type': '{{params.target_conn_type}}',
        }
    )
