"""Create resources for REP-292.

- Create Airflow connections (SFTP and Postgres).
- Create Postgres tables.
"""
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from pennymac_clicks.config import TABLE_EMPTY_KEYS, TABLE_NAME, TEMPLATE_SEARCHPATH, start_date
from pennymac_clicks.helpers.connection_helper import add_postgres_connection, add_sftp_connection
from pennymac_clicks.helpers.variable_helper import postgres_conn_id

logging.getLogger('boto').setLevel(logging.WARNING)

with DAG(
    dag_id='REP_292-create_resources',
    description='Create HomeStory SFTP and Postgres connections and S3 Bucket based on variables.',
    start_date=start_date,
    template_searchpath=TEMPLATE_SEARCHPATH,
    tags=['re_reporting', 'sftp']
) as dag:
    connections = DummyOperator(task_id='connections')
    tables = DummyOperator(task_id='tables')

    create_data_table = PostgresOperator(
        task_id='create_data_table',
        postgres_conn_id=postgres_conn_id(),
        sql='create_main_table.sql',
        params={'table_name': TABLE_NAME}
    )
    create_meta_table = PostgresOperator(
        task_id='create_meta_table',
        postgres_conn_id=postgres_conn_id(),
        sql='create_meta_table.sql',
        params={'table_empty_keys': TABLE_EMPTY_KEYS}
    )

    connections >> [add_sftp_connection(), add_postgres_connection()] >> tables
    tables >> [create_data_table, create_meta_table]
