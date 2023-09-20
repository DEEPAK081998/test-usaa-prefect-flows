import json
import logging
import os.path
import shutil
import subprocess
from http import HTTPStatus
from os import environ
from os import path, walk
from string import Template
from typing import Any, List
from zipfile import ZipFile

import boto3
import botocore.exceptions

import constants
import utils
from clients import airbyte_client as ab_client
from secrets.destination import DestinationSecretResolver
from secrets.source import SourceSecretResolver

logging.basicConfig(level=logging.INFO)
logging.getLogger('airbyte').setLevel(level=logging.INFO)


class AirbyteConnection:
    def __init__(
        self, api_base_url: str, table_name: str, connection_directory: str = None, connection_list=None,
        project_names=None, exclude=False, workspace_id: str = None, use_first_workspace: bool = False,
        test=False, airbyte_secrets_env_name: str = '', store_connection: bool = False, add_id_to_file: bool = False
    ) -> None:
        """
        Initialize the Airbyte connection class
        :param connection_directory: directory path where all connections are stored
        :param api_base_url: Airbyte api url
        :param table_name: Dynamodb table name where to store connection info
        :param workspace_id: airbyte workspace id
        :param use_first_workspace: whether to use first workspace if more than one workspace are found
        :param airbyte_secrets_env_name: name of the env variable that has airbyte connection file secret mappings
        :param test: whether to skips dynamodb connection
        :param store_connection: indicate whether to Store connection data in file
        """
        self.airbyte = ab_client.AirbyteAPIClass(base_url=api_base_url)
        self.store_connection = store_connection
        self.use_first_workspace = use_first_workspace
        self.workspace_id = workspace_id if workspace_id else self._initialize_workspace_id()
        self.source_definition_map = self._initialize_source_definition_map()
        self.destination_definition_map = self._initialize_destination_definition_map()
        self.connection_directory = connection_directory
        self.connection_list = connection_list
        self.project_name_list = project_names
        self.exclude = exclude
        self.test = test
        self.add_id_to_file = add_id_to_file
        self.dynamo_db_table = self._initialize_dynamo_db_table(table_name=table_name)
        self.connection_map = self._fetch_and_process_table_data()
        self.airbyte_secrets = json.loads(environ.get(airbyte_secrets_env_name, '{}'))

    def _initialize_source_definition_map(self) -> dict:
        """
        Creates a source definition map
        :return: a dict containing source definition name as key and source definition id as value
        """
        source_definition_list = self.airbyte.list_source_definitions()
        return {
            source_definition[constants.SOURCE_DEFINITION_ID]: source_definition['name'] for source_definition in
            source_definition_list
        } if self.store_connection else {
            source_definition['name']: source_definition[constants.SOURCE_DEFINITION_ID] for source_definition in
            source_definition_list
        }

    def _initialize_destination_definition_map(self) -> dict:
        """
        Creates a destination definition map
        :return: a dict containing destination definition name as key and destination definition id as value
        """
        destination_definition_list = self.airbyte.list_destination_definitions()
        return {
            destination_definition[constants.DESTINATION_DEFINITION_ID]: destination_definition['name']
            for destination_definition in destination_definition_list
        } if self.store_connection else {
            destination_definition['name']: destination_definition[constants.DESTINATION_DEFINITION_ID]
            for destination_definition in destination_definition_list
        }

    def _initialize_workspace_id(self) -> str:
        """
        Fetch the workspace id or create one if not exists
        :return: workspace id
        """
        workspace_list = self.airbyte.list_workspaces()
        workspace_id = None
        if len(workspace_list) == 0:
            logging.info('workspace does not exist creating now')
            body = constants.SAMPLE_WORKSPACE_DATA
            workspace_id = self.airbyte.create_workspace(body=body)[constants.WORKSPACE_ID]
        elif len(workspace_list) == 1 or self.use_first_workspace:
            if len(workspace_list) > 1:
                logging.info(f'got more than 1(actual={len(workspace_list)}) workspace selecting the first one')
            workspace_id = workspace_list[0][constants.WORKSPACE_ID]
        else:
            raise ValueError(
                f'got more than 1(actual={len(workspace_list)}) workspace please specify the workspace id to use in args'
            )
        logging.info(f'fetched workspace {workspace_id}')
        return workspace_id

    def _initialize_dynamo_db_table(self, table_name: str) -> boto3.resource:
        """
        Initialize the dynamodb table
        :param table_name: table name
        :return: dynamodb table object
        """
        if self.test:
            return None
        dynamodb = boto3.resource('dynamodb')
        return dynamodb.Table(table_name)

    def _fetch_and_process_table_data(self) -> dict:
        """
        Fetch and return the existing connection data
        :return: dict containing file name as key and connection id as value
        """
        if self.test:
            return {}
        response = self.dynamo_db_table.scan()
        if response['ResponseMetadata']['HTTPStatusCode'] != HTTPStatus.OK:
            raise ConnectionError(f'Response not OK, Got response {response}')
        table_items = response['Items']
        return {
            f'{row[constants.PRIMARY_KEY]}_{row[constants.SORT_KEY]}': row[constants.CONNECTION_ID]
            for row in table_items
        } if table_items else {}

    def _get_id_from_connection_map(self, project_name, file_name):
        key = f'{project_name}_{file_name}'
        return self.connection_map.get(key)

    def _create_airbyte_data(self, connection_dict: dict, project_name: str) -> dict:
        """
        Creates a dictionary having source, destination and connection data from connection data
        :param connection_dict: dict containing connection data
        :param project_name: project name
        :return: generated airbyte dict
        """
        source_data = json.load(
            open(
                os.path.join(
                    self.connection_directory,
                    project_name,
                    constants.SOURCES,
                    connection_dict[constants.SOURCE_CONNECTOR_FILE]
                )
            )
        )
        source_data = self._fill_env_secrets_in_dict(data=source_data)
        destination_data = json.load(
            open(
                os.path.join(
                    self.connection_directory,
                    project_name,
                    constants.DESTINATIONS,
                    connection_dict[constants.DESTINATION_CONNECTOR_FILE]
                )
            )
        )
        destination_data = self._fill_env_secrets_in_dict(data=destination_data)
        del connection_dict[constants.SOURCE_CONNECTOR_FILE]
        del connection_dict[constants.DESTINATION_CONNECTOR_FILE]
        return {
            constants.SOURCE: source_data,
            constants.DESTINATION: destination_data,
            constants.CONNECTION: connection_dict
        }

    def _create_source(
        self, source_dict: dict, workspace_id: str = None, source_id: str = None, update: bool = False
    ) -> dict:
        """
        Creates airbyte source from given data
        :param source_dict: source data dict
        :param workspace_id: workspace id
        :param source_id: source id (required during update)
        :param update: indicates whether to update the source or to create one
        :return: response dict
        """
        if update is True:
            source_dict[constants.SOURCE_ID] = source_id
            if source_dict.get(constants.SOURCE_DEFINITION_NAME):
                del source_dict[constants.SOURCE_DEFINITION_NAME]
            if source_dict.get(constants.SOURCE_DEFINITION_ID):
                del source_dict[constants.SOURCE_DEFINITION_ID]
            return self.airbyte.update_source(body=source_dict)
        else:
            if source_dict.get(constants.SOURCE_DEFINITION_NAME):
                source_dict[constants.SOURCE_DEFINITION_ID] = self.source_definition_map[
                    source_dict[constants.SOURCE_DEFINITION_NAME]]
                del source_dict[constants.SOURCE_DEFINITION_NAME]
            if (
                    not source_dict.get(constants.SOURCE_DEFINITION_NAME) and
                    not source_dict.get(constants.SOURCE_DEFINITION_ID)
            ):
                raise KeyError(
                    f'name or id not found please specify any of {constants.SOURCE_DEFINITION_NAME}'
                    f'or sourceDefinitionId'
                )
            if not source_dict.get(constants.WORKSPACE_ID):
                source_dict[constants.WORKSPACE_ID] = workspace_id if workspace_id else self.workspace_id
            return self.airbyte.create_source(body=source_dict)

    def _create_destination(
        self, dest_dict: dict, workspace_id: str = None, dest_id: str = None, update: bool = False
    ) -> dict:
        """
        Creates airbyte destination from given data
        :param dest_dict: destination data dict
        :param workspace_id: workspace id
        :param dest_id: destination id (required during update)
        :param update: indicates whether to update the destination or to create one
        :return: response dict
        """
        if update is True:
            dest_dict[constants.DESTINATION_ID] = dest_id
            if dest_dict.get(constants.DESTINATION_DEFINITION_NAME):
                del dest_dict[constants.DESTINATION_DEFINITION_NAME]
            if dest_dict.get(constants.DESTINATION_DEFINITION_ID):
                del dest_dict[constants.DESTINATION_DEFINITION_ID]
            return self.airbyte.update_destination(body=dest_dict)
        if dest_dict.get(constants.DESTINATION_DEFINITION_NAME):
            dest_dict[constants.DESTINATION_DEFINITION_ID] = self.destination_definition_map[
                dest_dict[constants.DESTINATION_DEFINITION_NAME]
            ]
            del dest_dict[constants.DESTINATION_DEFINITION_NAME]
        if not dest_dict.get(constants.DESTINATION_DEFINITION_NAME) and not dest_dict.get(
                constants.DESTINATION_DEFINITION_ID
        ):
            raise KeyError(
                f'name or id not found please specify any of {constants.DESTINATION_DEFINITION_NAME} '
                f'or {constants.DESTINATION_DEFINITION_ID}'
            )
        if not dest_dict.get(constants.WORKSPACE_ID):
            dest_dict[constants.WORKSPACE_ID] = workspace_id if workspace_id else self.workspace_id
        return self.airbyte.create_destination(body=dest_dict)

    def _create_connection(
        self, connection_dict: dict, source_id: str, destination_id: str, connection_id: str, update: bool = False
    ) -> dict:
        """
        Creates airbyte connection from given data
        :param connection_dict: connection data dict
        :param destination_id: destination id
        :param source_id: source id
        :param connection_id: connection id (required during update)
        :param update: indicates whether to update the connection or to create one
        :return: response dict
        """
        if update is True:
            if connection_dict.get('name'):
                del connection_dict['name']
            connection_dict[constants.CONNECTION_ID] = connection_id
            return self.airbyte.update_connection(body=connection_dict)
        else:
            connection_dict.update({constants.SOURCE_ID: source_id, constants.DESTINATION_ID: destination_id})
            return self.airbyte.create_connection(body=connection_dict)

    def _process_connection(self, connection_data: dict, project_name: str, file_name: str) -> None:
        """
        Process the airbyte connection
        :param connection_data: connection data
        :param project_name: project name
        :param file_name: file name which holds the connection data
        """
        logging.info(f'processing connection for file {file_name}')
        airbyte_data = self._create_airbyte_data(
            connection_dict=connection_data,
            project_name=project_name
        )
        workspace_id = connection_data.get(constants.WORKSPACE_ID, connection_data.get(constants.WORKSPACE))
        source_id = None
        destination_id = None
        update = False
        connection_id = self._get_id_from_connection_map(project_name=project_name, file_name=file_name)
        if connection_id:
            update = True
            connection_dict = self.airbyte.get_connection(connection_id=connection_id)
            source_id = connection_dict[constants.SOURCE_ID]
            destination_id = connection_dict[constants.DESTINATION_ID]
        source_id = self._create_source(
            source_dict=airbyte_data[constants.SOURCE],
            workspace_id=workspace_id,
            source_id=source_id,
            update=update
        )[constants.SOURCE_ID]
        destination_id = self._create_destination(
            dest_dict=airbyte_data[constants.DESTINATION],
            workspace_id=workspace_id,
            dest_id=destination_id,
            update=update
        )[constants.DESTINATION_ID]
        connection_id = self._create_connection(
            connection_dict=airbyte_data[constants.CONNECTION],
            connection_id=connection_id,
            source_id=source_id,
            destination_id=destination_id,
            update=update
        )[constants.CONNECTION_ID]
        if update is False and not self.test:
            self.dynamo_db_table.put_item(
                Item={
                    constants.PRIMARY_KEY: project_name,
                    constants.SORT_KEY: file_name,
                    constants.CONNECTION_ID: connection_id
                }
            )

    def _fill_env_secrets_in_dict(self, data: dict) -> dict:
        """
        Fill environment variable placeholders in dict
        :param data: data to fill placeholders in
        :return: response dict
        """
        json_string = json.dumps(data)
        template_class = Template(template=json_string)
        modified_json_string = template_class.safe_substitute(self.airbyte_secrets)
        return json.loads(modified_json_string)

    def _clean_source_data_for_file(self, source_data: dict) -> dict:
        """
        Cleans the source data file
        :param source_data: source data
        :return: cleaned source data
        """
        source_data[constants.SOURCE_DEFINITION_NAME] = self.source_definition_map.get(
            source_data.get(constants.SOURCE_DEFINITION_ID)
        )
        source_data = SourceSecretResolver().resolve_secrets(data_dict=source_data)
        keys = [constants.SOURCE_ID, constants.WORKSPACE_ID, constants.SOURCE_NAME, constants.SOURCE_DEFINITION_ID]
        for key in keys:
            del source_data[key]
        return source_data

    def _clean_destination_data_for_file(self, destination_data: dict) -> dict:
        """
        Cleans the destination data file
        :param destination_data: source data
        :return: cleaned destination data
        """
        destination_data[constants.DESTINATION_DEFINITION_NAME] = self.destination_definition_map.get(
            destination_data.get(constants.DESTINATION_DEFINITION_ID)
        )
        destination_data = DestinationSecretResolver().resolve_secrets(data_dict=destination_data)
        keys = [
            constants.DESTINATION_ID, constants.WORKSPACE_ID, constants.DESTINATION_NAME,
            constants.DESTINATION_DEFINITION_ID
        ]
        for key in keys:
            del destination_data[key]
        return destination_data

    def _clean_connection_data_for_file(self, connection_data: dict) -> dict:
        """
        Cleans the Connection data file
        :param connection_data: Connection data
        :return: cleaned Connection data
        """
        del connection_data[constants.SOURCE_ID]
        del connection_data[constants.DESTINATION_ID]
        del connection_data[constants.CONNECTION_ID]
        return connection_data

    def _fetch_and_process_connection(self, connection, connection_path, project_name):
        update = False
        connection_id = connection[constants.CONNECTION_ID]
        logging.info(f'processing connection {connection_id}')
        source_data = self.airbyte.get_source(source_id=connection[constants.SOURCE_ID])
        source_file_name = f'source_{source_data[constants.NAME]}'
        if self.add_id_to_file:
            source_file_name = f'{source_file_name}_{source_data[constants.SOURCE_ID]}'
        source_data = self._clean_source_data_for_file(source_data=source_data)

        destination_data = self.airbyte.get_destination(destination_id=connection[constants.DESTINATION_ID])
        destination_file_name = f'destination_{destination_data[constants.NAME]}'
        if self.add_id_to_file:
            destination_file_name = f'{destination_file_name}_{destination_data[constants.DESTINATION_ID]}'
        destination_data = self._clean_destination_data_for_file(destination_data=destination_data)

        connection_data = self._clean_connection_data_for_file(connection_data=connection)
        connection_data[constants.SOURCE_CONNECTOR_FILE] = f'{source_file_name}.json'
        connection_data[constants.DESTINATION_CONNECTOR_FILE] = f'{destination_file_name}.json'
        connection_file_name = f"connection_{source_data['name']}_{destination_data['name']}"
        if self.add_id_to_file:
            connection_file_name = f'{connection_file_name}_{connection_id}'
        stored_connection_id = self._get_id_from_connection_map(
            project_name=project_name,
            file_name=connection_file_name
        )

        if stored_connection_id and stored_connection_id != connection_id:
            logging.error(
                f'for file {connection_file_name} the connection id stored in dynamo and connection id fetched'
                f' from airbyte are diff {stored_connection_id} != {connection_id} ignoring this connection'
            )
            return
        elif stored_connection_id:
            update = True

        utils.put_data_dict_in_file(
            file_path=f"{os.path.join(connection_path, 'sources', source_file_name)}.json",
            data_dict=source_data
        )
        utils.put_data_dict_in_file(
            file_path=f"{os.path.join(connection_path, 'destinations', destination_file_name)}.json",
            data_dict=destination_data
        )
        utils.put_data_dict_in_file(
            file_path=f"{os.path.join(connection_path, 'connections', connection_file_name)}.json",
            data_dict=connection_data
        )

        if update is False and not self.test:
            self.dynamo_db_table.put_item(
                Item={
                    constants.PRIMARY_KEY: project_name,
                    constants.SORT_KEY: connection_file_name,
                    constants.CONNECTION_ID: connection_id
                }
            )

    def process_data(self):
        """
        Process the connection directory to register/update all airbyte connections
        """
        if self.store_connection:
            connection_list = self.airbyte.list_connections(self.workspace_id)
            connection_path = os.path.join(
                self.connection_directory,
                self.project_name_list[0]
            ) if self.project_name_list else self.connection_directory
            project_name = self.project_name_list[0] if self.project_name_list else (
                self.connection_directory.split('/')[-1]
            )
            if not project_name:
                raise ValueError(
                    f"the connection directory[{self.connection_directory}] don't have any project name"
                    f" specified in it, please specify the project name either by pointing to project "
                    f"through --connection-directory argument or by specifying the project name by --projects arg"
                )
            for connection in connection_list:
                if (
                        not self.connection_list or
                        (self.connection_list and (
                                (connection[constants.CONNECTION_ID] in self.connection_list and not self.exclude) or
                                (connection[constants.CONNECTION_ID] not in self.connection_list and self.exclude)
                        ))
                ):
                    self._fetch_and_process_connection(
                        connection=connection,
                        connection_path=connection_path,
                        project_name=project_name
                    )
        elif self.connection_list and not self.exclude:
            project_name = self.project_name_list[0]
            for file in self.connection_list:
                file_name = file.split('/')[-1].split('.')[0]
                connection_data = json.load(open(file))
                self._process_connection(
                    connection_data=connection_data,
                    file_name=file_name,
                    project_name=project_name
                )
        else:
            projects = next(walk(self.connection_directory))[1]
            for project in projects:
                if (
                        self.project_name_list is None or
                        (self.project_name_list and project in self.project_name_list and not self.exclude) or
                        (self.project_name_list and project not in self.project_name_list and self.exclude)
                ):
                    for root, dirs, files in walk(
                            os.path.join(self.connection_directory, project, constants.CONNECTIONS)
                    ):
                        for file in files:
                            file_path = path.join(root, file)
                            if (
                                    file.endswith('.json') and
                                    self.connection_list is None or
                                    (
                                            self.connection_list and
                                            file_path in self.connection_list and
                                            not self.exclude
                                    ) or
                                    (self.connection_list and file_path not in self.connection_list and self.exclude)
                            ):
                                file_name = file.split('.')[0]
                                connection_data = json.load(open(file_path))
                                self._process_connection(
                                    connection_data=connection_data,
                                    file_name=file_name,
                                    project_name=project
                                )


class AirbyteProcessor:
    def __init__(
        self, airbyte_api_url: str, zip_file_path: str, airbyte_directory: str = 'airbyte/',
        projects_directory: str = 'projects/', connection_directory: str = 'connection_config/',
        source_directory: str = os.getcwd(), projects: list = None, airbyte_secrets_env_name: str = '',
        env_file_path: str = f"{os.path.expanduser('~')}/.octavia", exclude: bool = False, test: bool = False,
        only_load_state_files: bool = False, only_upload_state_files: bool = False,
        api_header_file_path: str = 'api_http_headers.yaml'
    ) -> None:
        """
        Initializes the Airbyte processor class
        :param airbyte_api_url: Airbyte api url
        :param zip_file_path: Local or s3 path where to store state zip file
        :param airbyte_directory: Directory where all airbyte related files are stored
        :param projects_directory: Directory where all projects are stored
        :param connection_directory: Directory where all connections are stored
        :param source_directory: Project source directory
        :param projects: list of all project names to include/exclude
        :param airbyte_secrets_env_name: Name of the env variable that has airbyte connection file secret mappings
        :param env_file_path: Env file path
        :param exclude: exclude the provided connections/project
        :param test: Runs test only Skips state file related task
        :param only_load_state_files: When true then instead of deploying whole connections it only loads state files
            from zip_file_path into connection_directory
        :param only_upload_state_files: When true then instead of deploying whole connections it only
            uploads state files from connection_directory to zip_file_path
        :param api_header_file_path: api header file
        """
        self.airbyte_api_url = airbyte_api_url
        self.env_file_path = env_file_path
        self.zip_file_path = zip_file_path
        self.airbyte_secrets_env_name = airbyte_secrets_env_name
        self.airbyte_directory = airbyte_directory
        self.projects_directory = projects_directory
        self.connection_directory = connection_directory
        self.source_directory = source_directory
        self.project_name_list = projects
        self.exclude = exclude
        self.test = test
        self.only_load_state_files = only_load_state_files
        self.only_upload_state_files = only_upload_state_files
        self.api_header_file_path = api_header_file_path

    def _extract_state_file(self, zip_file_path: str) -> None:
        """
        Extract the zip file at project root
        :param zip_file_path: file path where zip file is located
        """
        logging.info(f'State file present at {self.zip_file_path} starting extraction')
        with ZipFile(zip_file_path, 'r') as zip_file:
            zip_file.extractall(self.source_directory)
        logging.info('Old state file extracted successfully')

    def _load_secrets_to_env_file(self) -> None:
        """
        Loads all the secrets to the env file
        """
        secrets_dict = json.loads(os.environ.get(self.airbyte_secrets_env_name, '{}'))
        logging.info(f'Loading secrets to file {self.env_file_path}')
        with open(self.env_file_path, "a") as file:
            for key, value in secrets_dict.items():
                value = json.dumps(value) if type(value) is dict else value
                file.write(f'{key}={value}\n')
        logging.info(f'Secrets loaded to file {self.env_file_path} successfully')

    def _load_state_files(self) -> None:
        """
        Load old state file to project root
        """
        if self.test:
            logging.info('Skipped loading of state file due to test mode')
            return
        if self.only_upload_state_files:
            return
        logging.info(f'Checking for state files')
        if self.zip_file_path.startswith(constants.S3_PATH_PREFIX):
            bucket, key = utils.extract_s3_uri(s3_uri=self.zip_file_path)
            try:
                utils.download_file_from_s3(file_name=constants.ZIP_FILE_TEMPORARY_PATH, bucket=bucket, key=key)
                self._extract_state_file(zip_file_path=constants.ZIP_FILE_TEMPORARY_PATH)
            except (FileNotFoundError, botocore.exceptions.ClientError) as e:
                logging.warning(f'Got error [{e}]')
                logging.info(f'No state file present at {self.zip_file_path} skipping extraction')
        else:
            if os.path.isfile(self.zip_file_path):
                self._extract_state_file(zip_file_path=self.zip_file_path)
            else:
                logging.info(f'No state file present at {self.zip_file_path} skipping extraction')

    def _deploy_connections(self) -> List:
        """
        Deploy airbyte connections
        :return: list of state files to zip
        """
        state_file_paths = []
        airbyte_projects = os.path.join(self.airbyte_directory, self.projects_directory)
        projects = set(
            next(walk(airbyte_projects))[1]
        )
        if self.project_name_list:
            # include/exclude projects provided in argument based on exclude arg
            projects = projects - set(self.project_name_list) if self.exclude else projects.intersection(
                set(self.project_name_list)
            )
        for project in projects:
            if not self.only_upload_state_files:
                logging.info(f'Deploying connections for project {project}')
            connection_path = os.path.join(
                self.source_directory,
                self.airbyte_directory,
                self.projects_directory,
                project,
                self.connection_directory
            )
            run_command = (
                f'docker run -i --rm -v {connection_path}:/home/octavia-project '
                f'--network host --env-file {self.env_file_path} --user $(id -u):$(id -g) airbyte/octavia-cli:0.40.4 '
                f'--airbyte-url {self.airbyte_api_url} --api-http-headers-file-path {self.api_header_file_path} '
                f'apply --force'
            )
            if not self.only_upload_state_files:
                subprocess.call(run_command, shell=True)
                logging.info(f'Deployment completed for project {project}')
            for root, directories, files in os.walk(connection_path):
                for filename in files:
                    if filename.startswith(constants.STATE_FILE_NAME_PREFIX):
                        filepath = os.path.join(root, filename)
                        logging.info(f'Saving file path {filepath} for zipping')
                        state_file_paths.append(filepath)
        return state_file_paths

    def _zip_and_upload_state_file(self, state_file_paths: List) -> None:
        """
        Zips and upload the latest state file
        :param state_file_paths: all states file path
        """
        if self.test:
            logging.info('Skipped uploading of state file due to test mode')
            return
        with ZipFile(constants.ZIP_FILE_TEMPORARY_PATH, 'w') as zip_file:
            for file in state_file_paths:
                logging.info(f'Zipping {file}')
                zip_file.write(file, os.path.relpath(file, self.source_directory))
        if self.zip_file_path.startswith(constants.S3_PATH_PREFIX):
            bucket, key = utils.extract_s3_uri(s3_uri=self.zip_file_path)
            utils.upload_file_to_s3(file_name=constants.ZIP_FILE_TEMPORARY_PATH, bucket=bucket, key=key)
        else:
            shutil.move(constants.ZIP_FILE_TEMPORARY_PATH, self.zip_file_path)
        logging.info(f'All files zipped successfully at location {self.zip_file_path}')

    def run(self) -> None:
        """
        Main run function
        """
        self._load_state_files()
        if self.only_load_state_files:
            return
        if self.airbyte_secrets_env_name:
            self._load_secrets_to_env_file()
        state_file_paths = self._deploy_connections()
        self._zip_and_upload_state_file(state_file_paths=state_file_paths)
