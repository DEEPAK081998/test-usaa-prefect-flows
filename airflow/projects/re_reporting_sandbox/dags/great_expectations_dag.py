import os
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Please add this env variable in order to run great expectations project in your dag.
# Great expectation pushes validation results and data docs to the s3 bucket name specified in
# the env variable. Add that bucket name whose read and write permissions are given to the
# airflow resource.
os.environ['MDP_S3_BUCKET'] = Variable.get('mdp-s3-bucket')

with DAG(
        'test_great_expectation_dag',
        description='Runs a test expectation on sandbox db: dbt_test2',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['great_expectations'],
) as dag:

    task_1 = GreatExpectationsOperator(
        task_id="great_expectation_example_task",
        data_context_root_dir='plugins/great_expectations',
        checkpoint_name="sandbox_db_checkpoint",
        return_json_dict = True,
    )
