import prefect
from prefect import Flow
from prefect import task
from prefect.engine import signals
import boto3
import json
import time

lambda_client = boto3.client('lambda')
dynamodb = boto3.client('dynamodb')
emr = boto3.client('emr')

start_cluster_lambda_function = 'vast-re-sandbox-dp-start-cluster'
record_stack_environment_table = 'RecordStackEnvironment'
agent_data_preparation_job = 'RecordStackSparkPreTamrCleanupApp'
agent_data_preparation_job_lock = 'RecordStackSparkPreTamrCleanupAppLock'

payload = {'table': record_stack_environment_table, 'key': agent_data_preparation_job}
json_payload = json.dumps(payload)

emr_termination_states = ['TERMINATED', 'TERMINATED_WITH_ERRORS']


@task
def batch_agent_data_preparation_job_invoke():
    response = lambda_client.invoke(
        FunctionName=start_cluster_lambda_function,
        InvocationType='RequestResponse',
        Payload=json_payload,
    )
    if 'FunctionError' in response:
        raise signals.FAIL(message='FunctionError present')


@task
def checking_preparation_job_status():
    lock_item = {}  # local variable for storing dynamoDB lock item - there is no lock in some moments when retry is on
    cluster_id_was_empty = False

    while True:
        lock_item_dynamodb = dynamodb.get_item(
            TableName=record_stack_environment_table,
            Key={
                'id': {
                    'S': agent_data_preparation_job_lock
                }
            },
            ConsistentRead=True
        )

        if 'Item' not in lock_item_dynamodb:
            if lock_item == {}:
                raise signals.FAIL(message='There is no Lock item in DynamoDB')
        else:
            lock_item = lock_item_dynamodb

        lock_payload_string = lock_item.get('Item').get('lockPayload').get('B').decode('ascii')
        lock_payload_json = json.loads(lock_payload_string)
        cluster_id = lock_payload_json['clusterId']
        retries_count = lock_payload_json['environmentConfiguration']['retries']

        #   when retry of EMR task is happening, it's possible to have moments when clusterId is still not present
        if not cluster_id:
            if not cluster_id_was_empty:
                cluster_id_was_empty = True
                time.sleep(60)
                continue
            else:
                raise signals.FAIL(message='There is no clusterId in DynamoDB Lock Item')

        cluster_info = emr.describe_cluster(ClusterId=cluster_id)
        if 'Cluster' not in cluster_info:
            raise signals.FAIL(message='There is no EMR cluster with Cluster id from Lock item')

        cluster_state = cluster_info.get('Cluster').get('Status').get('State')
        cluster_state_change_reason = cluster_info.get('Cluster').get('Status').get('StateChangeReason').get('Code')
        if cluster_state in emr_termination_states:
            if (cluster_state_change_reason.casefold() != 'ALL_STEPS_COMPLETED'.casefold()) and retries_count == 0:
                raise signals.FAIL(message='Cluster is in Termination state before ALL_STEPS_COMPLETED')
            elif cluster_state_change_reason.casefold() != 'ALL_STEPS_COMPLETED'.casefold():
                time.sleep(60)
                print('Next iteration of checking cluster status')
                continue
            else:
                print('TAMR Spark preparation job successfully completed')
                break
        else:
            time.sleep(60)
            print('Next iteration of checking cluster status')
            continue


flow_name = 'tamr-agent-deduplication'
with Flow(flow_name) as flow:
    checking_preparation_job_status(upstream_tasks=[batch_agent_data_preparation_job_invoke])
