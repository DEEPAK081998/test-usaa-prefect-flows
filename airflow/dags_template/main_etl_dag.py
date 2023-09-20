import json
import logging
import os
import re
import shutil
import time
import uuid
import zipfile
from datetime import datetime, timedelta
from dynamodb_json import json_util
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urlparse

import boto3
from airflow import AirflowException, DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.amazon.aws.operators.ecs import EcsOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow_dbt_python.operators.dbt import (
    DbtRunOperationOperator, DbtRunOperator, DbtSourceFreshnessOperator, DbtTestOperator,
)
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
from docker.types import Mount
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from airflow_plugins import get_airflow_dag_default_args
import airflow_retry_decorators as custom_airflow_decorators

import constants
from partner_email.config_helper import read_email_configuration
from partner_email.email_helper import send_emails_in_batches
from partner_email.s3_helper import read_csv_from_s3
from partner_email.test_helper import Tester

logging.basicConfig(level=logging.INFO)
logging.getLogger('airbyte').setLevel(level=logging.INFO)

EMPTY_TASKS_COUNTER = 0  # Counts total invocations of EmptyOperator
AIRBYTE_SYNC_TASKS_COUNTER = 0  # Counts total invocations of AirbyteTriggerSyncOperator
GREAT_EXPECTATIONS_TASKS_COUNTER = 0 # Counts total invocations of GreatExpectationsOperator


def create_empty_task() -> EmptyOperator:
    """
    Creates a empty operator task
    :return: A EmptyOperator instance
    """
    global EMPTY_TASKS_COUNTER
    EMPTY_TASKS_COUNTER += 1
    return EmptyOperator(task_id=f'empty_task_{EMPTY_TASKS_COUNTER}')


def attach_list_of_tasks(list_a: list, list_b: list) -> None:
    """
    Attach list of parallel tasks with a empty operator in between
    :param list_a: First list of tasks
    :param list_b: Second list of tasks
    """
    if list_a:
        empty_task = create_empty_task()
        list_a >> empty_task >> list_b


def sync_airbyte_connection(connection_id: str, **kwargs) -> AirbyteTriggerSyncOperator:
    """
    Syncs airbyte connection with the given connection id
    :param connection_id: Airbyte connection id
    :return: AirbyteTriggerSyncOperator instance
    """
    global AIRBYTE_SYNC_TASKS_COUNTER
    AIRBYTE_SYNC_TASKS_COUNTER += 1
    airbyte_extra_kwargs = kwargs['airbyte_extra_kwargs']
    airbyte_kwargs_map = dict(
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
    airbyte_kwargs_map.update(airbyte_extra_kwargs)
    return AirbyteTriggerSyncOperator(
        task_id=f'airbyte_conn_task_{AIRBYTE_SYNC_TASKS_COUNTER}',
        airbyte_conn_id='airbyte_connection',
        connection_id=connection_id,
        **airbyte_kwargs_map
    )


def sync_airbyte_connections_parallely(connection_ids: Union[str, List[str]], **kwargs) -> List[AirbyteTriggerSyncOperator]:
    '''
    Task to sync airbyte connections
    :param connection_ids: Airbyte connection id(s)
    :return: List of AirbyteTriggerSyncOperator instances
    '''
    if type(connection_ids) is str:
        return [sync_airbyte_connection(connection_ids, **kwargs)]
    return [sync_airbyte_connection(connection_id, **kwargs) for connection_id in connection_ids]


def execute_dbt_run_operation(**kwargs: Any):
    """
    Task to execute dbt run command
    :param kwargs: dictionary of keyword arguments
    """
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    dbt_kwargs_map = dict(
        task_id=f"dbt_run_operation_task",
        project_dir=kwargs['project_dir'],
        macro=kwargs['macro'],
        target=kwargs['target']
    )
    dbt_kwargs_map.update(dbt_extra_kwargs)
    dbt_run = DbtRunOperationOperator(**dbt_kwargs_map)
    return dbt_run


def execute_dbt_run(**kwargs):
    """
    Task to execute dbt run command
    :param kwargs (dict): dictionary of keyword arguments
    """
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    dbt_kwargs_map = dict(
        task_id=f"dbt_run_task",
        project_dir=kwargs['project_dir'],
        target=kwargs['target'],
        full_refresh=False,
        threads=25
    )
    dbt_kwargs_map.update(dbt_extra_kwargs)
    dbt_run = DbtRunOperator(**dbt_kwargs_map)
    return dbt_run


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


@custom_airflow_decorators.retry_mechanism
def _get_status(quicksight_client, aws_account_id: str, data_set_id: str, ingestion_id: str) -> str:
    """
    Get the current status of QuickSight Create Ingestion API.
    :param quicksight_client: quicksight boto client
    :param aws_account_id: An AWS Account ID
    :param data_set_id: QuickSight Data Set ID
    :param ingestion_id: QuickSight Ingestion ID
    :return: An QuickSight Ingestion Status
    """
    describe_ingestion_response = quicksight_client.describe_ingestion(
        AwsAccountId=aws_account_id, DataSetId=data_set_id, IngestionId=ingestion_id
    )
    return describe_ingestion_response["Ingestion"]["IngestionStatus"]


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


@custom_airflow_decorators.retry_mechanism
def run_single_quicksight_dataset(quicksight_client: boto3.client, aws_account_id: str, dataset_id: str) -> dict:
    """
    Process single quicksight dataset. (Retries throttling errors with exponential backoff)
    :param quicksight_client: Quicksight boto client
    :param aws_account_id: AWS account Id
    :param dataset_id: Dataset Id
    :return: Dict containing ingestion details
    """
    ingestion_id = str(uuid.uuid4())
    quicksight_client.create_ingestion(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_id,
        IngestionId=ingestion_id
    )
    return dict(aws_account_id=aws_account_id, data_set_id=dataset_id, ingestion_id=ingestion_id)


def run_quicksight_dataset_batch(quicksight_client: boto3.client, dataset_batch: list, aws_account_id: str) -> list:
    """
    Process batch of quicksight datasets
    :param quicksight_client: Quicksight boto client
    :param dataset_batch: List of quicksight dataset file names
    :param aws_account_id: AWS account Id
    :return: Ingestion details of given quicksight datasets
    """
    ingestion_details_list = []
    for dataset_id in dataset_batch:
        logging.info(f'Running dataset {dataset_id}')
        ingestion_detail = run_single_quicksight_dataset(
            quicksight_client=quicksight_client,
            aws_account_id=aws_account_id,
            dataset_id=dataset_id,
        )
        ingestion_details_list.append(ingestion_detail)
    return ingestion_details_list


def run_quicksight_datasets(datasets_per_batch=10, sleep_after_batch=5, **kwargs) -> None:
    """
    Operator to trigger quicksight datasets
    :param datasets_per_batch: Datasets to process in a single batch. (Default=10)
    :param sleep_after_batch: Sleep after processing a batch, in seconds. (Default=5)
    :param kwargs: additional kwargs
    """
    datasets_list = []
    aws_account_id = ''
    if kwargs.get('dataset_ids'):
        # if dataset ids are coming in kwargs, skip fetching from s3
        aws_account_id = kwargs['aws_account_id']
        datasets_list = kwargs['dataset_ids']
        logging.info(f'Refreshing datasets with ids: {datasets_list}')
    else:
        s3_client = boto3.client('s3')
        bucket_name = kwargs['s3_bucket_name']
        dataset_directory = kwargs['dataset_s3_directory']
        datasets_file_list = _get_objects_list(
            s3_client=s3_client,
            bucket_name=bucket_name,
            key_prefix=dataset_directory
        )
        logging.info(f'Fetched datasets file list {datasets_file_list}')
        for dataset_file in datasets_file_list:
            response_object = s3_client.get_object(Bucket=bucket_name, Key=dataset_file)
            dataset_dict = json.loads(response_object["Body"].read().decode())
            dataset_id = dataset_dict['DataSetId']
            aws_account_id = dataset_dict['AwsAccountId']
            datasets_list.append(dataset_id)

    ingestion_details_list = []
    quicksight_client = boto3.client('quicksight')
    for index in range(0, len(datasets_list), datasets_per_batch):
        logging.info(f'Processing dataset batch of length {datasets_per_batch} from index {index}')
        dataset_batch = datasets_list[index:index + datasets_per_batch]
        ingestion_details = run_quicksight_dataset_batch(
            quicksight_client=quicksight_client,
            dataset_batch=dataset_batch,
            aws_account_id=aws_account_id,
        )
        ingestion_details_list.extend(ingestion_details)
        time.sleep(sleep_after_batch)  # Add sleep after a batch
    for ingestion_detail in ingestion_details_list:
        _wait_for_state(
            quicksight_client=quicksight_client,
            target_state={"COMPLETED"},
            **ingestion_detail
        )


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
    run_command = kwargs['run_command']
    print(type(run_command))
    ecs_extra_kwargs_map = kwargs['ecs_extra_kwargs']
    print(type(ecs_extra_kwargs_map))
    conn = BaseHook.get_connection(conn_id)
    dbt_run = EcsOperator(
        task_id=f'ecs_run_task_{uuid.uuid4().hex[:4]}',
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
        **ecs_extra_kwargs_map
    )
    return dbt_run


def execute_docker_run(**kwargs) -> None:
    """
    Task to execute local docker image
    :param kwargs (dict): dictionary of keyword arguments
    """
    project_dir = kwargs['project_dir']
    conn_id = kwargs['conn_id']
    run_command = kwargs['run_command']
    docker_additional_envs = kwargs['docker_additional_envs']
    host_aws_cred_abs_path = kwargs['aws_cred_abs_path']
    image_name = kwargs['image_name']
    conn = BaseHook.get_connection(conn_id)
    mounts = []
    if host_aws_cred_abs_path:
        mounts.append(Mount(source=host_aws_cred_abs_path, target='/root/.aws/credentials', type='bind'))
    if 's3' != urlparse(project_dir).scheme:
        target = f"/usr/app/dbt/projects/{os.path.basename(project_dir).split('.zip')[0]}"
        mounts.append(Mount(source=project_dir, target=target, type='bind'))
        project_dir = target
    dbt_run = DockerOperator(
        task_id=f'dbt_local_docker_task_{uuid.uuid4().hex[:4]}',
        image=image_name,
        api_version='auto',
        docker_url='tcp://docker-proxy:2375',  # needed to connect to host machine
        command=run_command,
        mount_tmp_dir=True,
        network_mode='host',
        mounts=mounts if mounts else None,
        environment={
            'DBT_PROJECT_PATH': project_dir,
            'DB_NAME': conn.conn_id,
            'DB_HOST': conn.host,
            'DB_PASS': conn.password,
            'DB_PORT': f'{conn.port}',
            'DB_SCHEMA': conn.schema,
            'DB_TYPE': conn.conn_type,
            'DB_USER': conn.login,
            **docker_additional_envs
        }
    )
    return dbt_run


def execute_dbt_test_run(**kwargs):
    """
    Task to execute dbt test command
    :param kwargs (dict): dictionary of keyword arguments
    """
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    dbt_kwargs_map = dict(
        task_id=f"dbt_test_run_task",
        project_dir=kwargs['project_dir'],
        target=kwargs['target']
    )
    dbt_kwargs_map.update(dbt_extra_kwargs)
    dbt_run = DbtTestOperator(**dbt_kwargs_map)
    return dbt_run


def execute_dbt_source_freshness_run(**kwargs):
    """
    Task to execute dbt source freshness command
    :param kwargs (dict): dictionary of keyword arguments
    """
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    dbt_kwargs_map = dict(
        task_id=f"dbt_source_freshness_run_task",
        project_dir=kwargs['project_dir'],
        target=kwargs['target']
    )
    dbt_kwargs_map.update(dbt_extra_kwargs)
    dbt_run = DbtSourceFreshnessOperator(**dbt_kwargs_map)
    return dbt_run


def trigger_lambda_function_task(**kwargs):
    """
    Task to trigger lambda function
    :param kwargs (dict): dictionary of keyword arguments
    """
    lambda_trigger_extra_kwargs = kwargs['lambda_trigger_extra_kwargs']
    lambda_trigger_kwargs_map = dict(
        task_id=f"lambda_trigger_task",
        function_name=kwargs['function_name'],
        # controls whether to wait for lambda response. By default, it waits for a response
        invocation_type=kwargs['invocation_type']
    )
    lambda_trigger_kwargs_map.update(lambda_trigger_extra_kwargs)
    lambda_trigger_task = AwsLambdaInvokeFunctionOperator(**lambda_trigger_kwargs_map)
    return lambda_trigger_task


def send_partner_email(**kwargs):
    """
    Task to send emails to partners.
    :param kwargs (dict): dictionary of keyword arguments
    """
    partner_email = kwargs['partner_email']
    logging.info(f"partner_email_kwargs = {partner_email}")
    partner_email_variables = Variable.get('partner_email', deserialize_json=True)
    config = read_email_configuration(partner_email, partner_email_variables)

    send_emails_in_batches(
        postmark_api_secret=partner_email_variables['postmark_api_secret_id'],
        config=config,
        send_to=partner_email_variables['send_to'],
        limit=int(partner_email_variables['limit']),
        send=True)


def test_single_bank_data(**kwargs):
    """
    Task to test files in S3.
    Assert all bank names in `bank_name_col` are the same and equal to `bank_name`.
    :param kwargs (dict): dictionary of keyword arguments
    """

    test_params = kwargs['test_kwargs']
    print(f"test_params = {test_params}")
    if 'test_single_bank' in test_params:
        df = read_csv_from_s3(test_params['s3_bucket'], test_params['s3_key'])
        Tester.test_bank_name(df,
                              bank_name=test_params['test_single_bank']['bank_name'],
                              bank_name_col=test_params['test_single_bank']['bank_name_col'])
    else:
        print('Skipped test because it was not requested.')


def extract_s3_uri_path(s3_uri: str) -> Tuple[str, str]:
    """
    function that extracts the s3 bucket and s3 key from the s3_uri
    :param s3_uri (str): s3_uri to the object
    """
    parsed = urlparse(s3_uri)
    return (parsed.netloc, parsed.path[1:])

def download_file_from_s3(s3_bucket_name: str, file_path: str, local_file_path: str ) -> None:
    """
    function that downloads the file from s3
    :param s3_bucket_name (str): the name of the s3 bucket
    :param file_path (str): the path to the s3 object
    :param local_file_path (str): the path to store the file from s3 on mwaa/airflow
    """
    client = boto3.client('s3')
    client.download_file(Key=file_path, Bucket=s3_bucket_name, Filename=local_file_path)
    logging.info("File Successfully downloaded from S3 Bucket")

def download_and_extract_from_s3(s3_uri: str) -> None:
    """
    function that downloads the zip file from s3 and extracts it into the /tmp folder
    :param s3_uri (str): s3_uri to the object
    """
    s3_bucket, s3_key = extract_s3_uri_path(s3_uri=s3_uri)
    local_zip_file_path = f'{constants.LOCAL_GX_PROJECT_PATH}{uuid.uuid4().hex[:8]}.zip'
    download_file_from_s3(s3_bucket_name=s3_bucket, file_path=s3_key, local_file_path=local_zip_file_path)
    # extract the downloaded zip file in the /tmp
    project_dir = os.path.splitext(os.path.basename(s3_key))[0]
    with zipfile.ZipFile(local_zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(f'{constants.LOCAL_GX_TMP_PATH}{project_dir}')

def resolve_great_expectations_project(project_dir: str) -> str:
    """
    function that downloads the zip file from s3 or picks it from local system and loads it into the /tmp folder
    :param project_dir (str): the s3_uri of project or the local path to gx_project
    """
    if project_dir.startswith('s3://'):
        download_and_extract_from_s3(s3_uri=project_dir)
        project_dir = os.path.splitext(os.path.basename(project_dir))[0]
    logging.info("Great Expectations Project has been moved to /tmp")
    return f'{constants.LOCAL_GX_TMP_PATH}{project_dir}'

def execute_great_expectations_task(**kwargs):
    """
    function that loads the greatexpectations project into the /tmp folder and executes the GreatExpectations task
    """
    project_dir = kwargs['project_path']
    # download the zip file from s3 or pick from local system and load it into the /tmp folder
    resolved_project_path = resolve_great_expectations_project(project_dir)
    gx_extra_kwargs = kwargs['great_expectations_extra_kwargs']
    gx_kwargs_map = dict(
        task_id="gx_operator_task",
        checkpoint_name=kwargs['checkpoint'],
        data_context_root_dir=resolved_project_path,
        return_json_dict=True
    )
    gx_kwargs_map.update(gx_extra_kwargs)
    gx_validate_pg = GreatExpectationsOperator(**gx_kwargs_map)
    gx_validate_pg.execute(dict())


def create_great_expectations_tasks(task_configuration: str):
    global GREAT_EXPECTATIONS_TASKS_COUNTER
    GREAT_EXPECTATIONS_TASKS_COUNTER += 1
    great_expectations_task = PythonOperator(
        task_id=f'great_expectations_task{GREAT_EXPECTATIONS_TASKS_COUNTER}',
        python_callable=execute_great_expectations_task,
        op_kwargs={
            'project_path': task_configuration.get('great_expectations_project_path'),
            'checkpoint': task_configuration.get('great_expectations_checkpoint'),
            'great_expectations_extra_kwargs': task_configuration.get('great_expectations_extra_kwargs', {})
        }
    )
    return great_expectations_task


def execute_great_expectations_tasks_parallely(task_configurations: Union[str, List[str]], **kwargs) -> List[PythonOperator]:
    '''
    Task to execute great expectations
    :param task_configurations: GreatExpectations task configurations
    :return: List of PythonOperator instances
    '''
    task_list = []

    def append_task(configuration):
        if configuration.get('great_expectations_project_path') and configuration.get('great_expectations_checkpoint'):
            task_list.append(create_great_expectations_tasks(configuration, **kwargs))

    if isinstance(task_configurations, dict):
        append_task(task_configurations)
    else:
        for task_configuration in task_configurations:
            append_task(task_configuration)

    return task_list


def _update_config_params_map(
    original_config_params_map: Dict, resolve_from_variable: bool = False, resolve_from_dynamo: bool = False, resolve_timeout: bool = False
) -> Dict:
    """
    Updates config param
    :param original_config_params_map: Original config params
    :param resolve_from_variable: indicates whether to resolve config params from variable
    :param resolve_from_dynamo: indicates whether to resolve config params from dynamodb
    :return: Modified config params
    """
    updated_config_params_map = {}

    if resolve_from_variable is True:
        updated_config_params_map = Variable.get(f'{file_name}_extra_params', deserialize_json=True, default_var={})
    if resolve_from_dynamo is True:
        dynamo_db_config_table_name = Variable.get(f'dynamodb_config_table_name', default_var='')
        client = boto3.client('dynamodb')
        if not dynamo_db_config_table_name:
            logging.warning('no dynamodb table name provided skipping config resolution')
        else:
            try:
                response = client.get_item(
                    TableName=dynamo_db_config_table_name,
                    Key={'dag_name': {'S': f'{file_name}'}}
                )
                extra_params = response.get('Item', {})
                if not extra_params:
                    logging.warning(f'no row found with dag name {file_name}, cant apply config')
                else:
                    updated_config_params_map = json_util.loads(extra_params)
            except client.exceptions.ResourceNotFoundException:
                logging.warning(
                    f'no dynamodb table found with name {dynamo_db_config_table_name} skipping config resolution'
                )
    # typecasts the execution_timeout from integer to timedelta
    # The execution_timeout keyword will be a part of the extra-kwargs for the task being created
    if resolve_timeout is True:
        for parameters in updated_config_params_map.values():
            if isinstance(parameters, dict) and "execution_timeout" in parameters:
                parameters["execution_timeout"] = timedelta(seconds=parameters["execution_timeout"])
    original_config_params_map.update(updated_config_params_map)
    return original_config_params_map


file_name = os.path.splitext(os.path.basename(__file__))[0]
CONFIG_PARAMS_MAP: dict = {{CONFIG_PARAMS_MAP_PLACEHOLDER}}
CONFIG_PARAMS_MAP = _update_config_params_map(
    original_config_params_map=CONFIG_PARAMS_MAP,
    resolve_from_variable=True,
    resolve_from_dynamo=True,
    resolve_timeout=True
)

with DAG(
        file_name,
        description='Runs airbyte connections and then triggers a DBT task',
        schedule_interval=CONFIG_PARAMS_MAP.get('schedule'),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        max_active_runs=CONFIG_PARAMS_MAP.get('max_active_runs', 16),
        default_args=get_airflow_dag_default_args(),  # default args include task failure alert etc.
        tags=CONFIG_PARAMS_MAP.get('tags')
) as dag:
    task_scheduler = ''

    lambda_name: str = CONFIG_PARAMS_MAP.get('lambda_name')
    if lambda_name:
        op_kwargs = {
            'function_name': lambda_name,
            'invocation_type': (
                'RequestResponse' if CONFIG_PARAMS_MAP.get('wait_for_lambda_response', True)
                else 'Event'
            ),
            'lambda_trigger_extra_kwargs': CONFIG_PARAMS_MAP.get('lambda_trigger_extra_kwargs', None) or {}
        }
        lambda_trigger_task = trigger_lambda_function_task(**op_kwargs)
        attach_list_of_tasks(list_a=task_scheduler, list_b=lambda_trigger_task)
        task_scheduler = lambda_trigger_task

    airbyte_connection_ids: list = CONFIG_PARAMS_MAP.get('connection_ids')
    if airbyte_connection_ids:
        op_kwargs = {
            'airbyte_extra_kwargs': CONFIG_PARAMS_MAP.get('airbyte_extra_kwargs', None) or {}
        }
        for connection_ids in airbyte_connection_ids:
            airbyte_sync_tasks = sync_airbyte_connections_parallely(connection_ids=connection_ids, **op_kwargs)
            attach_list_of_tasks(list_a=task_scheduler, list_b=airbyte_sync_tasks)
            task_scheduler = airbyte_sync_tasks

    if CONFIG_PARAMS_MAP.get('dbt_project_path') and CONFIG_PARAMS_MAP.get('dbt_conn_id'):
        common_op_kwargs = {
            'project_dir': CONFIG_PARAMS_MAP.get('dbt_project_path'),
            'target': CONFIG_PARAMS_MAP.get('dbt_conn_id')
        }
        if CONFIG_PARAMS_MAP.get('run_dbt', True) is True:
            op_kwargs = {
                'dbt_extra_kwargs': CONFIG_PARAMS_MAP.get('dbt_extra_kwargs', None) or {},
                **common_op_kwargs
            }
            dbt_run_task = execute_dbt_run(**op_kwargs)
            attach_list_of_tasks(list_a=task_scheduler, list_b=dbt_run_task)
            task_scheduler = dbt_run_task
        if CONFIG_PARAMS_MAP.get('run_dbt_test', False) is True:
            op_kwargs = {
                'dbt_extra_kwargs': CONFIG_PARAMS_MAP.get('dbt_test_extra_kwargs', None) or {},
                **common_op_kwargs
            }
            dbt_test_run_task = execute_dbt_test_run(**op_kwargs)
            attach_list_of_tasks(list_a=task_scheduler, list_b=dbt_test_run_task)
            task_scheduler = dbt_test_run_task
        if CONFIG_PARAMS_MAP.get('run_dbt_source_freshness', False) is True:
            op_kwargs = {
                'dbt_extra_kwargs': CONFIG_PARAMS_MAP.get('dbt_source_freshness_extra_kwargs', None) or {},
                **common_op_kwargs
            }
            dbt_source_run_task = execute_dbt_source_freshness_run(**op_kwargs)
            attach_list_of_tasks(list_a=task_scheduler, list_b=dbt_source_run_task)
            task_scheduler = dbt_source_run_task
        if CONFIG_PARAMS_MAP.get('dbt_conn_macro'):
            op_kwargs={
                'dbt_extra_kwargs': CONFIG_PARAMS_MAP.get('dbt_operations_extra_kwargs', None) or {},
                'macro': CONFIG_PARAMS_MAP.get('dbt_conn_macro'),
                **common_op_kwargs
            }
            dbt_run_operation_task = execute_dbt_run_operation(**op_kwargs)
            attach_list_of_tasks(list_a=task_scheduler, list_b=dbt_run_operation_task)
            task_scheduler = dbt_run_operation_task

    if CONFIG_PARAMS_MAP.get('s3_bucket_name'):
        dataset_run_task = PythonOperator(
            task_id='dataset_run_task',
            python_callable=run_quicksight_datasets,
            **CONFIG_PARAMS_MAP.get('quicksight_data_run_extra_kwargs', None) or {},
            op_kwargs={
                's3_bucket_name': CONFIG_PARAMS_MAP.get('s3_bucket_name'),
                'dataset_s3_directory': CONFIG_PARAMS_MAP.get('dataset_s3_directory'),
            }
        )
        attach_list_of_tasks(list_a=task_scheduler, list_b=dataset_run_task)
        task_scheduler = dataset_run_task

    if CONFIG_PARAMS_MAP.get('aws_account_id') and CONFIG_PARAMS_MAP.get('quicksight_dataset_ids'):
        dataset_run_task = PythonOperator(
            task_id='dataset_refresh_task',
            python_callable=run_quicksight_datasets,
            **CONFIG_PARAMS_MAP.get('quicksight_data_refresh_extra_kwargs', None) or {},
            op_kwargs={
                'aws_account_id': CONFIG_PARAMS_MAP.get('aws_account_id'),
                'dataset_ids': CONFIG_PARAMS_MAP.get('quicksight_dataset_ids')
            }
        )
        attach_list_of_tasks(list_a=task_scheduler, list_b=dataset_run_task)
        task_scheduler = dataset_run_task

    if CONFIG_PARAMS_MAP.get('ecs_cluster_name'):
        op_kwargs = {
            'run_command': CONFIG_PARAMS_MAP.get('ecs_run_command'),
            'project_dir': CONFIG_PARAMS_MAP.get('dbt_project_path'),
            'conn_id': CONFIG_PARAMS_MAP.get('dbt_conn_id'),
            'cluster_name': CONFIG_PARAMS_MAP.get('ecs_cluster_name'),
            'container_name': CONFIG_PARAMS_MAP.get('ecs_container_name'),
            'task_arn': CONFIG_PARAMS_MAP.get('ecs_task_arn'),
            'ecs_extra_kwargs': CONFIG_PARAMS_MAP.get('ecs_extra_kwargs')
        }
        ecs_run_task = execute_ecs_run(**op_kwargs)
        attach_list_of_tasks(list_a=task_scheduler, list_b=ecs_run_task)
        task_scheduler = ecs_run_task
    if CONFIG_PARAMS_MAP.get('run_local_docker'):
        op_kwargs = {
            'run_command': CONFIG_PARAMS_MAP.get('ecs_run_command'),
            'project_dir': CONFIG_PARAMS_MAP.get('dbt_project_path'),
            'conn_id': CONFIG_PARAMS_MAP.get('dbt_conn_id'),
            'image_name': CONFIG_PARAMS_MAP.get('local_docker_image_name'),
            'aws_cred_abs_path': CONFIG_PARAMS_MAP.get('local_aws_cred_abs_path'),
            'docker_additional_envs': CONFIG_PARAMS_MAP.get('local_docker_additional_envs'),
        }
        ecs_run_task = execute_docker_run(**op_kwargs)
        attach_list_of_tasks(list_a=task_scheduler, list_b=ecs_run_task)
        task_scheduler = ecs_run_task

    if CONFIG_PARAMS_MAP.get('partner_email'):
        partner_email_task = PythonOperator(
            task_id='send_partner_email',
            python_callable=send_partner_email,
            **CONFIG_PARAMS_MAP.get('partner_email_extra_kwargs', None) or {},
            op_kwargs={
                'partner_email': CONFIG_PARAMS_MAP.get('partner_email'),
            }
        )
        attach_list_of_tasks(list_a=task_scheduler, list_b=partner_email_task)
        task_scheduler = partner_email_task

    if CONFIG_PARAMS_MAP.get('copy_from_s3_to_sftp'):
        from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator

        copy_params = CONFIG_PARAMS_MAP['copy_from_s3_to_sftp']
        test_single_bank_data_task = PythonOperator(
            task_id='test_single_bank_data',
            python_callable=test_single_bank_data,
            **CONFIG_PARAMS_MAP.get('copy_from_s3_to_sftp_extra_kwargs', None) or {},
            op_kwargs={
                'test_kwargs': CONFIG_PARAMS_MAP.get('copy_from_s3_to_sftp'),
            }
        )
        copy_from_s3_to_sftp_task = S3ToSFTPOperator(
            task_id='copy_from_s3_to_sftp',
            s3_bucket=copy_params['s3_bucket'],
            s3_key=copy_params['s3_key'],
            sftp_path=copy_params['sftp_path'],
            sftp_conn_id=copy_params['sftp_conn_id'],
        )
        test_single_bank_data_task >> copy_from_s3_to_sftp_task

    if CONFIG_PARAMS_MAP.get('great_expectations_project_path') and CONFIG_PARAMS_MAP.get('great_expectations_checkpoint'):
        great_expectations_task = PythonOperator(
            task_id='great_expectations_task',
            python_callable=execute_great_expectations_task,
            op_kwargs={
                'project_path': CONFIG_PARAMS_MAP.get('great_expectations_project_path'),
                'checkpoint' : CONFIG_PARAMS_MAP.get('great_expectations_checkpoint'),
                'great_expectations_extra_kwargs' : CONFIG_PARAMS_MAP.get('great_expectations_extra_kwargs', {})
            }
        )
        attach_list_of_tasks(list_a=task_scheduler, list_b=great_expectations_task)
        task_scheduler = great_expectations_task
    
    if CONFIG_PARAMS_MAP.get('great_expectations_task_configurations'):
        for task_configuration in CONFIG_PARAMS_MAP.get('great_expectations_task_configurations'):
            great_expectations_task = execute_great_expectations_tasks_parallely(task_configurations=task_configuration)
            #  verify that task has been created
            if great_expectations_task:
                attach_list_of_tasks(list_a=task_scheduler, list_b=great_expectations_task)
                task_scheduler = great_expectations_task
