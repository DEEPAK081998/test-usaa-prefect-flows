import os

from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

file_name = os.path.splitext(os.path.basename(__file__))[0]

with DAG(
    file_name,
    description='A simple hello world DAG',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['test_project'],
) as dag:

    BashOperator(
        task_id='print_hello_world',
        bash_command="echo 'hello world'",
    )
