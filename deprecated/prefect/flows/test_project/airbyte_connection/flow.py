from prefect import Flow
from prefect import Parameter

from tasks import sync_airbyte_connection

flow_name = 'airbyte-connection'
sync_airbyte_connection = sync_airbyte_connection()
with Flow(flow_name) as flow:
    connection_id = Parameter('connection_id')
    airbyte_server_host = Parameter('host', default='localhost')
    airbyte_server_port = Parameter('port', default=8000)
    sync_airbyte_connection(
        airbyte_server_host=airbyte_server_host,
        airbyte_server_port=airbyte_server_port,
        airbyte_api_version='v1',
        connection_id=connection_id
    )
