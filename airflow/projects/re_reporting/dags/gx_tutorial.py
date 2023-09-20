import os
import pandas as pd
import sys
from airflow.models import Variable
from airflow.models.baseoperator import chain
import logging
from pathlib import Path
from pendulum import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)


base_path = Path(__file__).parents[2]
# data_dir = base_path / "include" / "great_expectations"
# data_file = data_dir / "yellow_tripdata_sample_2019-01.csv"

ge_root_dir = Variable.get('GE_DATA_CONTEXT_ROOT_DIR')
POSTGRES_CONN_ID = "pg_expectations_connector"

logging.basicConfig(level=logging.INFO)
logging.info(base_path)
logging.info(sys.path)
logging.info(ge_root_dir)


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)

def gx_tutorial():
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS strawberries;
            CREATE TABLE IF NOT EXISTS strawberries (
                id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100),
                amount INT
            );

            INSERT INTO strawberries (id, name, amount)
            VALUES ('001', 'Strawberry Order 1', 10),
                ('002', 'Strawberry Order 2', 5),
                ('003', 'Strawberry Order 3', 8),
                ('004', 'Strawberry Order 4', 3),
                ('005', 'Strawberry Order 5', 12);
            """,
    )



    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir=ge_root_dir,
        checkpoint_name="checkpoint_berry",
        data_asset_name="strawberries",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )

    drop_table_pg = PostgresOperator(
        task_id="drop_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE strawberries;
            """,
    )

    # gx_validate_pg
    create_table_pg >> gx_validate_pg >> drop_table_pg
#

gx_tutorial()
