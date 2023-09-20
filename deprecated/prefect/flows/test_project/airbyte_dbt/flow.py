from datetime import timedelta

from prefect import Flow, Parameter, task, config
from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.tasks.secrets.base import PrefectSecret

flow_name = 'airbyte-dbt-flow'

sync_airbyte_connection = AirbyteConnectionTask(
    max_retries=3,
    retry_delay=timedelta(seconds=20),
)

run_dbt = DbtShellTask(
    environment='dev',
    profile_name='testrun',
    set_profiles_envar=False,
    max_retries=3,
    retry_delay=timedelta(seconds=20),
    overwrite_profiles=True,
    return_all=True,
    log_stderr=True
)


@task
def getSecretFromParam(param: str) -> dict:
    """
    Function to fetch a prefect secret corresponding to 'param' key
    :param param: secret key that prefect should look for
    """
    secretTask = PrefectSecret(param)
    secret = secretTask.run()
    return secret


with Flow(flow_name) as flow:
    # In order to fetch secrets from prefect cloud, following flag should be marked
    # 'False' in Prefect config
    config.cloud.use_local_secrets = False

    connection_id = Parameter('connection_id')
    airbyte_host = Parameter('airbyte_host', default='localhost')
    airbyte_port = Parameter('airbyte_port', default=8000)
    dbt_command = Parameter('dbt_command', default='dbt run --project-dir /dbt-test')
    helper_script = Parameter('dbt_helper_script', default=None)
    dbt_kwargs_key = Parameter('dbt_kwargs_key')

    dbt_kwargs_secret = getSecretFromParam(dbt_kwargs_key)

    airbyte_connection_sync = sync_airbyte_connection(
        airbyte_server_host=airbyte_host,
        airbyte_server_port=airbyte_port,
        airbyte_api_version='v1',
        connection_id=connection_id
    )
    run_dbt(
        command=dbt_command,
        helper_script=helper_script,
        dbt_kwargs=dbt_kwargs_secret,
        upstream_tasks=[airbyte_connection_sync],
    )
