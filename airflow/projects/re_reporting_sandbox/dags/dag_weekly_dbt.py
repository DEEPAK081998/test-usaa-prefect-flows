import os, ast

from datetime import datetime

from airflow import DAG

from airflow.models import Variable

from airflow.models.param import Param

from airflow.operators.python import PythonOperator

from airflow_dbt_python.operators.dbt import DbtRunOperator

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from airflow_plugins import get_airflow_dag_default_args

# TODO: Add this in ENG-270
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# TODO: Add this in ENG-270
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
#     'connection_test',
# ]

# TODO: Add the code below in ENG-270
# def get_connection_ids(**kwargs):
#     '''
#     Task to get airbyte connection ids from DynamoDB
#     :param kwargs (dict): dictionary of keyword arguments
#     '''
#     ti = kwargs["ti"]
#     dynamo_db_table_name = kwargs['table_name']
#     project_name = kwargs['project_name']

#     dynamodb = AwsBaseHook(client_type='dynamodb').get_conn()
#     connection_ids = []
#     for connection_file_name in connection_files_name_list:
#         table_data = dynamodb.get_item(
#             TableName=dynamo_db_table_name,
#             Key={'project_name': {'S': project_name}, 'file_name': {'S': connection_file_name}}
#         )
#         connection_ids.append(table_data['Item']['connectionId']['S'])

#     # pushing connection ids to XCom
#     ti.xcom_push(key='connection_ids', value=connection_ids)

# def sync_airbyte_connections(ti):
#     '''
#     Task to sync airbyte connections
#     :param ti: airflow task instance object
#     '''
#     # xcom pull
#     connection_ids = ti.xcom_pull(key='connection_ids', task_ids='get_connection_ids')
#     for connection_id in connection_ids:
#         airbyte_connection_sync = AirbyteTriggerSyncOperator(
#             connection_id=connection_id,
#             task_id='airbyte_conn_example',
#             airbyte_conn_id='airbyte_connection',
#             asynchronous=False,
#             timeout=3600,
#             wait_seconds=3
#         )
#         airbyte_connection_sync.execute(dict())


def execute_dbt_run(**kwargs):
    '''
    Task to execute dbt run command
    :param kwargs (dict): dictionary of keyword arguments
    '''
    dbt_extra_kwargs = kwargs['dbt_extra_kwargs']
    project_dir = kwargs['project_dir']
    target = kwargs['target']

    dbt_extra_kwargs_map = ast.literal_eval(dbt_extra_kwargs)
    dbt_run = DbtRunOperator(
        task_id="dbt_run_example",
        project_dir=project_dir,
        target=target,
        threads=25,
        select=["+marts"],
        #exclude=["tag:mlo_invites+"],
        full_refresh=False,
        vars={"incremental_catch_up": "True"},
        **dbt_extra_kwargs_map
    )
    dbt_run.execute(dict())
    

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
        # TODO: Add this IN ENG-270
        # 'table_name': Param('', description='Dynamo DB table name', type='string'),
        # 'project_name': Param('', description='Dyname DB project name', type='string'),

        # TODO: Delete 'connection_id' param in ENG-270
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
    },
) as dag:

    dbt_run_task = PythonOperator(
        task_id='dbt_run_task',
        python_callable=execute_dbt_run,
        op_kwargs={
            'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
            'project_dir': '{{params.dbt_project_path}}',
            'target': '{{ params.dbt_conn_id }}',
        }
    )

    # Sets dbt_run as a downstream task to airbyte_connection_sync hence, dbt_run will run
    # after airbyte_connection_sync
    dbt_run_task

    # TODO: Add the code below in ENG-270
    # # Task to fetch connection ids from DynamoDB and push them to Xcom
    # connection_ids_task = PythonOperator(
    #     task_id='get_connection_ids',
    #     python_callable=get_connection_ids,
    #     provide_context=True,
    #     op_kwargs={
    #         'table_name':'{{ params.table_name }}',
    #         'project_name':'{{ params.project_name }}',
    #     },
    # )

    # # Gets connection ids from Xcom and triggers airbyte connection syncs
    # airbyte_connections_task = PythonOperator(
    #     task_id='sync_airbyte_connections',
    #     python_callable=sync_airbyte_connections,
    #     provide_context=True,
    # )

    # # Runs a DBT project
    # dbt_run_task = PythonOperator(
    #     task_id='dbt_run_task',
    #     python_callable=execute_dbt_run,
    #     op_kwargs={
    #         'dbt_extra_kwargs': '{{ params.dbt_extra_kwargs}}',
    #         'project_dir':'{{params.dbt_project_path}}',
    #         'target':'{{ params.dbt_conn_id }}',
    #     }
    # )

    # # Sets upstream and downstream tasks
    # connection_ids_task >> airbyte_connections_task >> dbt_run_task
