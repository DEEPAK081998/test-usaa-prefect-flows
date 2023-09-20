from datetime import timedelta

from prefect import config, Flow, Parameter, task, unmapped
from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.tasks.secrets.base import PrefectSecret
from prefect.utilities.aws import get_boto_client

flow_name = 're_reporting_db_dw_flow'
# TODO: Add back in ENG-270
# connection_files_name_list = [
#     'connection_fidelity_goals_events_Production DataWarehouse_ff9a8d16-93b2-46e2-9cb8-ad88fd0f54b0',
#     'connection_chase-rep60-test-fullrefresh-overwrite_Production DataWarehouse_268c4174-3487-4227-8b4f-e7727351dba1',
#     'connection_lakeview_goals_events_Production DataWarehouse_81c3acaf-201f-4278-a5ef-1cbb15ad0f55',
#     'connection_citizens-ga-rep60_Production DataWarehouse_4c42564b-f97c-4d04-809c-a378df553752',
#     'connection_freedom-sendgrid-invitations_Production DataWarehouse_4f67ec75-a17b-45ed-b93c-f77407300c8c',
#     'connection_alliant-spr-clicks_Production DataWarehouse_53499fa9-ec33-4108-b12a-236a06c397ba',
#     'connection_alliant-rep60-user-counts_Production DataWarehouse_c241849e-adc5-4ebe-9340-4e979fd87030',
#     'connection_HS Read Replica_Production DataWarehouse_991f2219-4f05-4d93-9569-a55fc646cf9d',
#     'connection_sofi-paths-with-source_Production DataWarehouse_519baff8-85a5-4deb-914a-109f95845446',
#     'connection_tms-users-rep60_Production DataWarehouse_c77395ee-b351-4322-a48e-0ffb03bfcf3c',
#     'connection_citizens_goals_events_Production DataWarehouse_84becbb5-ca43-4557-b1a3-7b1ce33267c3',
#     'connection_alliant-daily-invitations-sg_Production DataWarehouse_132a664b-b3f1-4f49-9eb6-a1bc9f1d7a64',
#     'connection_sofi-goals-data_Production DataWarehouse_4eb5f418-c11a-4bde-95fe-b2d606e1ecd6',
#     'connection_chase-ga-rep60_Production DataWarehouse_6b70e41f-2255-4553-b8de-50623626d3d1',
#     'connection_freedom-ga-rep60_Production DataWarehouse_537a6ae3-0508-4947-90f5-183444579312',
#     'connection_freedom-cta-teambuilder-goals_Production DataWarehouse_937d15d5-486e-44a0-8b10-717de7f3ca21',
#     'connection_sofi-custom-dimension_Production DataWarehouse_e448825c-8985-4c75-82be-985e336bd9f2',
#     'connection_sofi-rep60-users_Production DataWarehouse_ecc52750-6a73-4837-aee7-749e3dd71114',
#     'connection_sofi-test_Production DataWarehouse_33828a09-76fb-447b-a09a-a2e4add70513',
#     'connection_Dynamo Invitations_Production DataWarehouse_6ff4b50f-e7db-4c4e-8f3b-d2b4b9566738',
#     'connection_lakeview-users_Production DataWarehouse_270951df-7ab7-4d50-ab03-a4bf0b11f319',
#     'connection_goals-test_Production DataWarehouse_5b209974-71d3-46b8-9a6b-84d1a6b9c605',
#     'connection_sofi-campaign-data_Production DataWarehouse_14b7e5e9-0212-4ba7-a1c6-e067f99bfdc8',
#     'connection_alliant-sendgrid-invitations_Production DataWarehouse_197531bf-c650-4b7c-b1d6-aff58bb34a31',
#     'connection_sofi-custom-dimension_Production DataWarehouse_691a73be-8f18-4815-941d-e9ce4017b092',
#     'connection_freedom-ga-rep60_Production DataWarehouse_4db6d0c6-dde7-43fb-8653-09945e391c04',
#     'connection_sofi-test-metrics_Production DataWarehouse_c0991825-14ce-4884-8030-65a780c3d570',
#     'connection_sofi-test_Production DataWarehouse_97058ad9-3886-45eb-abfe-8052705833e2',
#     'connection_freedom-daily-invitations-sg_Production DataWarehouse_86a61a6a-fd2c-4344-965e-f0de5e4b1f1c',
#     'connection_AuthService_Production DataWarehouse_c9daef29-2f11-4220-839a-894ae188c8f2',
#     'connection_freedom-utm-content_Production DataWarehouse_cddc933e-9606-46ed-80dc-e79b7b8b8f66',
#     'connection_rbc_goals_events_Production DataWarehouse_a71237a7-8156-4c0b-938d-12277a7ca6cc',
#     'connection_sofi-ga-prod-data_Production DataWarehouse_979663a1-e2a3-4fe7-b6fe-0c4b7d7fd14c',
#     'connection_freedom_goals_events_Production DataWarehouse_a8bb79bb-17ac-4db8-9aa0-940a806b978c',
#     'connection_tms-ga-rep60_Production DataWarehouse_0d1c55df-dd8d-4026-99e2-b63bb455012b',
#     'connection_sofi_lead_submissions_Production DataWarehouse_268d5f18-ad5b-49fa-914c-0ff110c7fa8e',
#     'connection_sofi-ga-data-general_Production DataWarehouse_cbcf0d80-759c-42c7-88b2-3757241777ce',
#     'connection_citizens-users-rep60_Production DataWarehouse_1689006e-6b74-4d97-a855-74c8041bd44f',
#     'connection_Twilio - HomeStory_Production DataWarehouse_ee67fbcf-d794-4d26-b770-623c8d00c56a',
#     'connection_sofi_metrics_Production DataWarehouse_7fae773e-536d-4aa7-9d91-7ad8176ed411',
#     'connection_fidelity-ga-rep60_Production DataWarehouse_bda89641-84ce-4ed0-827a-4a7e77000fb1',
#     'connection_citizens-ga-data-rep60-users_Production DataWarehouse_da26b249-4cd7-4170-b239-dcccb9304d83',
#     'connection_rbc-users-rep60_Production DataWarehouse_a2d995ac-6f6c-48dc-8dc5-1cb7e0814e28',
#     'connection_fidelity-ga-rep60-users_Production DataWarehouse_422f37ee-4e95-4473-a9ba-30a675d14f0c',
#     'connection_alliant_goals_events_Production DataWarehouse_b7d1617b-e001-4c8f-a7a7-d2d59b9593d1',
#     'connection_chase_goals_events_Production DataWarehouse_38a82445-1128-43f1-a1b1-240d91608b12',
#     'connection_rbc-ga-rep60_Production DataWarehouse_902e576a-4b12-4b1f-ade8-90f7169cf8fa',
#     'connection_tms_goals_events_Production DataWarehouse_8b05ff38-afe9-438b-a3b8-1da32f46fc4e',
#     'connection_send-test2_file-test2_70594519-9af4-4d71-b26b-f5a8f6e7262a',
#     'connection_alliant-ga-rep60_Production DataWarehouse_0960a47d-e7ec-4d08-919a-d7155644aa7c',
#     'connection_lakeview-data-general_Production DataWarehouse_56e2c585-609a-4ca0-a7de-37ff26146396'
# ]

sync_airbyte_connection = AirbyteConnectionTask(
    max_retries=3,
    retry_delay=timedelta(seconds=20),
)

run_dbt = DbtShellTask(
    environment='dev',
    profile_name='re_reporting_dw_fix',
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
    secret_task = PrefectSecret(param)
    secret = secret_task.run()
    return secret


# TODO: Add back in ENG-270
# @task
# def get_connection_ids(connection_file_name, project_name, dynamo_db_table_name):
#     dynamodb = get_boto_client("dynamodb")
#     table_data = dynamodb.get_item(
#         TableName=dynamo_db_table_name,
#         Key={'project_name': {'S': project_name}, 'file_name': {'S': connection_file_name}}
#     )
#     return table_data['Item']['connectionId']['S']


with Flow(flow_name) as flow:
    # In order to fetch secrets from prefect cloud, following flag should be marked
    # 'False' in Prefect config
    config.cloud.use_local_secrets = False

    # TODO: Remove connection_id in ENG-270
    connection_id = Parameter('connection_id')

    airbyte_host = Parameter('airbyte_host', default='localhost')
    airbyte_port = Parameter('airbyte_port', default=8000)
    dbt_command = Parameter('dbt_command', default='dbt run --project-dir /re_reporting_dw_fix')
    helper_script = Parameter('dbt_helper_script', default=None)
    dbt_kwargs_key = Parameter('dbt_kwargs_key')

    # TODO: Add back in ENG-270
    # table_name = Parameter('table_name')
    # project_name = Parameter('project_name')

    dbt_kwargs_secret = getSecretFromParam(dbt_kwargs_key)

    # TODO: Remove in ENG-270
    airbyte_connection_sync = sync_airbyte_connection(
        airbyte_server_host=airbyte_host,
        airbyte_server_port=airbyte_port,
        airbyte_api_version='v1',
        connection_id=connection_id
    )

    # TODO: Add back in ENG-270
    # connection_ids = get_connection_ids.map(
    #     connection_file_name=connection_files_name_list,
    #     project_name=unmapped(project_name),
    #     dynamo_db_table_name=unmapped(table_name)
    # )
    # airbyte_connection_tasks = sync_airbyte_connection.map(
    #     airbyte_server_host=unmapped(airbyte_host),
    #     airbyte_server_port=unmapped(airbyte_port),
    #     airbyte_api_version=unmapped('v1'),
    #     connection_id=connection_ids
    # )

    run_dbt(
        command=dbt_command,
        helper_script=helper_script,
        dbt_kwargs=dbt_kwargs_secret,
        upstream_tasks=[airbyte_connection_sync],  # TODO: Remove in ENG-270
        # upstream_tasks = [airbyte_connection_tasks] # TODO: Add back in ENG-270
    )
