import json
import logging
from typing import List

from airflow import settings
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.connection import Connection
from pennymac_clicks.helpers.aws_helper import read_secret
from pennymac_clicks.helpers.variable_helper import (postgres_conn_id, postgres_secret_id,
                                                     sftp_conn_id, sftp_secret_id)


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


@task
def add_postgres_connection():
    """Add Postgres connection to Airflow."""

    POSTGRES_CONN_ID = postgres_conn_id()
    secret = read_secret(postgres_secret_id())
    connection = Connection(
        conn_id=POSTGRES_CONN_ID,
        conn_type='postgres',
        host=secret['host'],
        login=secret['username'],
        password=secret['password'],
        schema=secret['dbname'],
        port=secret['port']
    )
    _add_connection(connection)


@task
def add_sftp_connection():
    """Add SFTP connection to Airflow."""

    def fix_private_key(private_key: str) -> str:
        """Fix private key by adding '\n' new line characters before and after the actual key.
        https://stackoverflow.com/a/73478593
        """

        fixed_private_key = (
            private_key
            .replace('KEY-----', 'KEY-----\n')
            .replace('-----END', '\n-----END')
        )
        return fixed_private_key

    secret = read_secret(sftp_secret_id())
    private_key = fix_private_key(secret['private_key'])
    extra = json.dumps({
        'private_key': private_key,
        'no_host_key_check': True
    })
    connection = Connection(
        conn_id=sftp_conn_id(),
        conn_type='sftp',
        host=secret['host'],
        login=secret['username'],
        extra=extra
    )
    _add_connection(connection)
