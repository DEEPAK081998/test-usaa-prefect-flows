import ast
import json
import logging
import os
import time
import uuid
from datetime import datetime

import boto3
from airflow import AirflowException, DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow_plugins import get_airflow_dag_default_args
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logging.getLogger('airbyte').setLevel(level=logging.INFO)


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
        full_refresh=False,
        threads=25,
        # exclude=["tag:mlo_invites+"],
        **dbt_extra_kwargs_map
    )
    dbt_run.execute(dict())


def _get_objects_list(s3_client: boto3.client, bucket_name: str, key_prefix: str, **kwargs) -> list:
    """
    return the object list
    :param s3_client: s3 client
    :param bucket_name: bucket name
    :param key_prefix: path prefix
    :param kwargs: addition kwargs
    :return: list of folders/files fetched from given bucket and path
    """
    list_kwargs = dict(Bucket=bucket_name, Prefix=key_prefix, **kwargs)
    is_truncated = True
    folder_list = []
    while is_truncated:
        response = s3_client.list_objects_v2(**list_kwargs)
        list_kwargs['ContinuationToken'] = response.get('NextContinuationToken')
        is_truncated = response.get('IsTruncated', False)
        if response['KeyCount'] == 0:
            return folder_list
        if kwargs.get('Delimiter'):
            folder_list = folder_list + [
                prefix_dict['Prefix'] for prefix_dict in response['CommonPrefixes']
            ]
        else:
            folder_list = folder_list + [
                object_dict['Key'] for object_dict in response['Contents']
            ]
    return folder_list


def _get_status(
    quicksight_client, aws_account_id: str, data_set_id: str, ingestion_id: str, max_retry_count: int = 10
) -> str:
    """
    Get the current status of QuickSight Create Ingestion API.
    :param quicksight_client: quicksight boto client
    :param aws_account_id: An AWS Account ID
    :param data_set_id: QuickSight Data Set ID
    :param ingestion_id: QuickSight Ingestion ID
    :param max_retry_count: Max retry count if throttling error occurs (Default=10)
    :return: An QuickSight Ingestion Status
    """
    retry_count = 0
    while retry_count <= max_retry_count:
        try:
            describe_ingestion_response = quicksight_client.describe_ingestion(
                AwsAccountId=aws_account_id, DataSetId=data_set_id, IngestionId=ingestion_id
            )
            return describe_ingestion_response["Ingestion"]["IngestionStatus"]
        except KeyError:
            raise AirflowException("Could not get status of the Amazon QuickSight Ingestion")
        except ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                sleep_duration = 30 + (2 ** retry_count)
                retry_count += 1
                logging.info(f'Retrying dataset {data_set_id} after {sleep_duration} seconds.')
                time.sleep(sleep_duration)
            else:
                raise exception
    raise Exception(f'Max retries exceeded for dataset: {data_set_id}')


def _wait_for_state(
    quicksight_client: boto3.client, aws_account_id: str, data_set_id: str, ingestion_id: str, target_state: set,
    check_interval: int = 30
) -> str:
    """
    Check status of a QuickSight Create Ingestion API
    :param quicksight_client: quicksight boto client
    :param aws_account_id: An AWS Account ID
    :param data_set_id: QuickSight Data Set ID
    :param ingestion_id: QuickSight Ingestion ID
    :param target_state: Describes the QuickSight Job's Target State
    :param check_interval: the time interval in seconds which the operator
        will check the status of QuickSight Ingestion
    :return: response of describe_ingestion call after Ingestion is done
    """
    sec = 0
    status = _get_status(quicksight_client, aws_account_id, data_set_id, ingestion_id)
    while status in {"INITIALIZED", "QUEUED", "RUNNING"} and status != target_state:
        logging.info(f"Dataset {data_set_id} Current status is %s", status)
        time.sleep(check_interval)
        sec += check_interval
        if status in {"FAILED"}:
            raise AirflowException(f"The Amazon QuickSight Ingestion failed for dataset {data_set_id}!")
        if status == "CANCELLED":
            raise AirflowException(f"The Amazon QuickSight SPICE ingestion cancelled for dataset {data_set_id}!")
        status = _get_status(quicksight_client, aws_account_id, data_set_id, ingestion_id)

    logging.info(f"QuickSight Ingestion for dataset {data_set_id} completed")
    return status


def run_single_quicksight_dataset(
    quicksight_client: boto3.client, aws_account_id: str, dataset_id: str, max_retry_count: int = 10
) -> dict:
    """
    Process single quicksight dataset. (Retries throttling errors with exponential backoff)
    :param quicksight_client: Quicksight boto client
    :param aws_account_id: AWS account Id
    :param dataset_id: Dataset Id
    :param max_retry_count: Max retry count if throttling error occurs (Default=10)
    :return: Dict containing ingestion details
    """
    success = False
    retry_count = 0
    ingestion_id = str(uuid.uuid4())
    while not success and retry_count <= max_retry_count:
        try:
            quicksight_client.create_ingestion(
                AwsAccountId=aws_account_id,
                DataSetId=dataset_id,
                IngestionId=ingestion_id
            )
            success = True
        except ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                # Add exponential backoff in case throttling error occurs
                sleep_duration = 30 + (2 ** retry_count)
                retry_count += 1
                logging.info(f'Retrying dataset {dataset_id} after {sleep_duration} seconds.')
                time.sleep(sleep_duration)
            else:
                raise exception
    if not success:
        raise Exception(f'Max retries exceeded for dataset: {dataset_id}')
    return dict(aws_account_id=aws_account_id, data_set_id=dataset_id, ingestion_id=ingestion_id)


def run_quicksight_dataset_batch(
    quicksight_client: boto3.client, s3_client: boto3.client, bucket_name: str, dataset_batch: list
) -> list:
    """
    Process batch of quicksight datasets
    :param quicksight_client: Quicksight boto client
    :param s3_client: S3 boto client
    :param bucket_name: Name of the s3 bucket from where to fetch quicksight dataset
    :param dataset_batch: List of quicksight dataset file names
    :return: Ingestion details of given quicksight datasets
    """
    ingestion_details_list = []
    for dataset_file in dataset_batch:
        logging.info(f'Fetching file from S3 {dataset_file}')
        response_object = s3_client.get_object(Bucket=bucket_name, Key=dataset_file)
        dataset_dict = json.loads(response_object["Body"].read().decode())
        dataset_id = dataset_dict['DataSetId']
        aws_account_id = dataset_dict['AwsAccountId']
        logging.info(f'Running dataset {dataset_id}')
        ingestion_detail = run_single_quicksight_dataset(quicksight_client=quicksight_client, aws_account_id=aws_account_id, dataset_id=dataset_id)
        ingestion_details_list.append(ingestion_detail)
    return ingestion_details_list


def run_quicksight_datasets(datasets_per_batch=10, sleep_after_batch=5, **kwargs) -> None:
    """
    Operator to trigger quicksight datasets
    :param datasets_per_batch: Datasets to process in a single batch. (Default=10)
    :param sleep_after_batch: Sleep after processing a batch, in seconds. (Default=5)
    :param kwargs: additional kwargs
    """
    s3_client = boto3.client('s3')
    quicksight_client = boto3.client('quicksight')
    bucket_name = kwargs['s3_bucket_name']
    dataset_directory = kwargs['dataset_s3_directory']
    dataset_file_list = _get_objects_list(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key_prefix=dataset_directory
    )
    logging.info(f'Fetched datasets list {dataset_file_list}')
    ingestion_details_list = []
    for index in range(0, len(dataset_file_list), datasets_per_batch):
        logging.info(f'Processing dataset batch of length {datasets_per_batch} from index {index}')
        dataset_batch = dataset_file_list[index:index + datasets_per_batch]
        ingestion_details = run_quicksight_dataset_batch(
            quicksight_client=quicksight_client,
            s3_client=s3_client,
            bucket_name=bucket_name,
            dataset_batch=dataset_batch
        )
        ingestion_details_list.extend(ingestion_details)
        # Add sleep after a batch
        time.sleep(sleep_after_batch)
    for ingestion_detail in ingestion_details_list:
        _wait_for_state(
            quicksight_client=quicksight_client,
            target_state={"COMPLETED"},
            **ingestion_detail
        )


file_name = os.path.splitext(os.path.basename(__file__))[0]

with DAG(
        file_name,
        description='Runs airbyte connections and then triggers a DBT task',
        schedule_interval=Variable.get(f'{file_name}_schedule', default_var=None),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        default_args=get_airflow_dag_default_args(), # default args include task failure alert etc.
        tags=['dbt', 'airbyte', 're_reporting'],
        params={
            'connection_id': Param(
                '991f2219-4f05-4d93-9569-a55fc646cf9d', description='Connection ID on airbyte', type='string',
            ),
            'dbt_project_path': Param(
                's3://mdp-prod-bucket/dbt/projects/re_reporting_dw_fix.zip',
                description='S3 Path of the zipped dbt project directory',
                type='string'
            ),
            'dbt_conn_id': Param('warehouse_db', description='DBT connection object ID on airflow', type='string'),
            'dbt_extra_kwargs': Param({}, description='Extra kwargs to include while running DBT run', type='object'),
            's3_bucket_name': Param(
                'mdp-prod-bucket',
                description='S3 Path of the zipped dbt project directory',
                type='string'
            ),
            'dataset_s3_directory': Param(
                'quicksight/datasets/re_reporting/',
                description='S3 Path of the zipped dbt project directory',
                type='string'
            ),
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

    dbt_run_task = PythonOperator(
        task_id='dbt_run_task',
        python_callable=execute_dbt_run,
        op_kwargs={
            'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
            'project_dir': '{{params.dbt_project_path}}',
            'target': '{{ params.dbt_conn_id }}',
        }
    )
    dataset_run_task = PythonOperator(
        task_id='dataset_run_task',
        python_callable=run_quicksight_datasets,
        op_kwargs={
            's3_bucket_name': '{{ params.s3_bucket_name}}',
            'dataset_s3_directory': '{{params.dataset_s3_directory}}',
        }
    )

    # Sets dbt_run as a downstream task to airbyte_connection_sync hence, dbt_run will run
    # after airbyte_connection_sync
    airbyte_connection_sync >> dbt_run_task >> dataset_run_task
