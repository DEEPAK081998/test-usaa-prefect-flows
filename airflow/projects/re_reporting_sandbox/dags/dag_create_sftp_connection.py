import datetime
import json
import logging
from typing import List

import boto3
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.connection import Connection
from botocore.exceptions import ClientError

from airflow import DAG, settings
from airflow.models.param import Param


def list_connections() -> List[str]:
    """Get Airflow connections names."""

    session = settings.Session()
    connections = session.query(Connection).all()
    connection_names = [connection.conn_id for connection in connections]
    return connection_names


def _add_connection(connection: Connection):
    """Add Airflow connection."""

    if connection.conn_id in list_connections():
        logging.info(f"Connection already exist: {connection.conn_id}")
    else:
        try:
            session = settings.Session()
            session.add(connection)
            session.commit()
        except Exception as e:
            raise AirflowSkipException(f"Failed to add connection. Exception: {e}.")


def read_secret(secret_id):
    """Read secret from AWS Secrets Manager."""

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_id
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    if isinstance(secret, str) and '"' in secret:
        logging.info('Secret is string. Using json.loads() to convert to dict.')
        return json.loads(secret)
    return secret


@task
def add_sftp_connection(**context):
    """Add SFTP connection to Airflow."""

    def fix_private_key(private_key: str) -> str:
        """Fix private key by adding '\n' new line characters before and after the actual key.
        https://stackoverflow.com/a/73478593
        """

        print(f"len(private_key) = {len(private_key)}")
        print(f"private_key[:10] = {private_key[:10]}")

        fixed_private_key = (
            private_key
            .replace('KEY-----', 'KEY-----\n')
            .replace('-----END', '\n-----END')
        )
        return fixed_private_key

    sftp_secret_id = context['params']['sftp_secret_id']
    secret = read_secret(sftp_secret_id)
    print(f"secret.keys() = {secret.keys()}")
    private_key = fix_private_key(secret['private_key'])
    extra = json.dumps({
        'private_key': private_key,
        'no_host_key_check': True
    })
    partition = context['params']['partition']
    print(f"partition = {partition}")
    conn_id = f"sftp_hs_{partition}"
    connection = Connection(
        conn_id=conn_id,
        conn_type='sftp',
        host=secret['host'],
        login=partition,
        extra=extra
    )
    _add_connection(connection)
    test_result = connection.test_connection()
    success = test_result[0]
    if not success:
        raise Exception(f"Failed connection test. Connection id: {conn_id}")
    print(f"test_result = {test_result}")
    print(f"Created the following connection: {conn_id}")


with DAG(
        dag_id="create_sftp_connection",
        start_date=datetime.datetime(2021, 1, 1),
        schedule_interval=None,
        params={
            'partition': Param('partition', type='string', description='SFTP partition/login'),
            'sftp_secret_id': Param('homestory_airflow_sftp_private_key', type='string', description='SFTP private key secret ID'),
        },
        tags=['gui', 'sftp']):

    add_sftp_connection()
