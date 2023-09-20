import os
import json
import zipfile
import fileinput
import base64
import shutil
import requests
import docker
import logging
import boto3
import docker.models.images
from os import path, walk
from typing import Tuple, Union

import constants as airflow_constants
import utils as airflow_utils

logging.basicConfig(level=logging.INFO)
logging.getLogger('airflow').setLevel(level=logging.INFO)


class Airflow:
    def __init__(
        self, s3_bucket: str = None, s3_key: str = None, projects: str = None, dags: str = None,
        dags_directory: str = None, test: bool = False, exclude: bool = False, requirements_file: str = None,
        s3_requirements_path: str = None, plugins_directory: str = None, s3_plugins_path: str = None,
        airflow_stack_name: str = None, docker_image_tag: str = None, docker_file_dir: str = None,
        not_mark_latest: bool = False, docker_build_args: dict = None, ecs_cluster_name: str = None,
        ecs_service: str = None, update_ecs_services: bool = False, update_variables: str = None,
        projects_directory: str = None, update_dynamic_dags: bool = False, update_common_dags: bool = False,
        common_dags_directory: str = None, update_standalone_dags: bool = False, mdp_utils_stack_name: str = None,
        docker_dir_names: str = None, airflow_resource_logical_id: str = None, local_dag_dir: str = None
    ) -> None:
        """
        Initialize the Airflow connection class
        :param s3_bucket: S3 Bucket name where to store all dags
        :param s3_key: Path is S3 bucket where to store the dags
        :param projects: list of all project names for which dags needed to be created/updated
        :param dags: list of all dags files path which needed to be created/updated
        :param dags_directory: Directory where all dags are stored
        :param test: Runs test only Skips dags upload
        :param exclude: exclude the provided dags/project
        :param requirements_file: Path to the requirements.txt file for airflow
        :param s3_requirements_path: Full path in S3 bucket where to store the requirements file
        :param plugins_directory: Directory where all plugins are stored
        :param s3_plugins_path: Full path in S3 bucket where to store the pulgins zip file
        :param airflow_stack_name: Name of airflow stack
        :param docker_image_tag: Docker image tag to assign
        :param docker_file_dir: Directory where docker file is present
        :param docker_mark_latest: Will not mark the docker image as latest on ECR
        :param docker_build_args: Any additional build args needed for image in form of KEY=VALUE format
        :param ecs_cluster_name: ECS cluster name where airflow is deployed
        :param ecs_service: ECS service name
        :param update_service: Whether to update ecs services or not
        :param variables_file: Path to variables files that needs to be updated
        :param projects_directory: Directory where all projects are stored
        :param update_dynamic_dags: Whether to update dynamic dags or not
        :param update_common_dags: Whether to update common dags or not
        :param common_dags_directory: Directory where all common dags are stored
        :param update_standalone_dags: Whether to update standalone project dags or not
        :param mdp_utils_stack_name: Name of mdp utils stack
        :param docker_dir_name_list: List of all docker dirs names which needs to be included
        :param airflow_resource_logical_id: Logical id of airflow resource provided in cloudformation template
        :param local_dag_dir: path to the directory where dags will be stored on the local machine
        """
        self.test = test
        self.exclude = exclude
        self.s3_key = s3_key
        self.project_name_list = projects
        self.dags_list = dags
        self.dags_directory = dags_directory
        self.requirements_path = requirements_file
        self.s3_requirements_path = s3_requirements_path
        self.plugins_directory = plugins_directory
        self.s3_plugins_path = s3_plugins_path
        self.airflow_stack_name = airflow_stack_name
        self.s3_bucket = self._initialize_s3_bucket(s3_bucket_name=s3_bucket)
        self.docker_image_tag = docker_image_tag
        self.docker_file_dir = docker_file_dir
        self.docker_mark_latest = not not_mark_latest
        self.docker_build_args = docker_build_args
        self.ecs_cluster_name = ecs_cluster_name
        self.ecs_service = ecs_service
        self.update_service = update_ecs_services
        self.variables_file = update_variables
        self.projects_directory = projects_directory
        self.update_dynamic_dags = update_dynamic_dags
        self.update_common_dags = update_common_dags
        self.common_dags_directory = common_dags_directory
        self.update_standalone_dags = update_standalone_dags
        self.mdp_utils_stack_name = mdp_utils_stack_name
        self.docker_dir_name_list = docker_dir_names
        self.docker_client, self.ecr_registry = self._connect_to_ecr()
        self.airflow_resource_logical_id = airflow_resource_logical_id or airflow_constants.AIRFLOW_RESOURCE_LOGICAL_ID
        self.mwaa_env_name = self._get_mwaa_environment_name() if self.airflow_stack_name else None
        self.local_dag_dir = local_dag_dir

    def _connect_to_ecr(self) -> Tuple[Union[docker.client.DockerClient, None], Union[str, None]]:
        """
        Connect to aws ecr repo
        :return: docker client and connected registry
        """
        if not self.mdp_utils_stack_name:
            return None, None

        docker_client = docker.from_env()
        if self.test:
            return docker_client, 'test'
        client = boto3.client('ecr')
        token = client.get_authorization_token()

        logging.info(f'CONNECTED TO ECR')

        b64token = token['authorizationData'][0]['authorizationToken'].encode('utf-8')
        username, password = base64.b64decode(b64token).decode('utf-8').split(':')
        registry = token['authorizationData'][0]['proxyEndpoint']
        docker_client.login(username='AWS', password=password, registry=registry)
        return docker_client, registry

    def _initialize_s3_bucket(self, s3_bucket_name) -> boto3.resource:
        """
        Initialize the S3 bucket object
        :param s3_bucket_name: S3 Bucket name
        :return: boto client resource
        """
        if self.test or not s3_bucket_name:
            return None
        s3 = boto3.resource('s3')
        return s3.Bucket(s3_bucket_name)

    def _initialize_mwaa_client(self) -> boto3.client:
        """
        Initialize MWAA client
        :return: MWAA client
        """
        if self.test:
            return None
        return boto3.client('mwaa')

    def _initialize_cloudformation_client(self) -> boto3.client:
        """
        Initialize cloudformation client
        :return: cloudformation client
        """
        if self.test:
            return None
        return boto3.client('cloudformation')

    def _get_mwaa_environment_name(self) -> str:
        """
        Get name of the MWAA resource from airflow stack
        :return: MWAA resource name
        """
        cloudformation_client = self._initialize_cloudformation_client()
        response = cloudformation_client.describe_stack_resource(
            StackName=self.airflow_stack_name,
            LogicalResourceId=self.airflow_resource_logical_id
        )
        return response['StackResourceDetail']['PhysicalResourceId']

    def _get_object_version(self, obj_key: str) -> str:
        """
        Return object version on S3
        :param obj_key: key of the object on S3
        :return: Object version
        """
        version_collection = self.s3_bucket.object_versions.filter(Prefix=obj_key)
        for version in version_collection:
            return version.get().get('VersionId')

    def _update_mwaa_env(self, identical_plugin_files: bool, identical_requirement_files: bool) -> None:
        """
        Update MWAA environment if there is change in either requirements or plugins file
        :param identical_plugin_files: indicate whether plugin files are identical or not
        :param identical_requirement_files: indicate whether requirement files are identical or not
        """
        if not identical_plugin_files or not identical_requirement_files:
            mwaa_client = self._initialize_mwaa_client()
            update_request_params = {'Name': self.mwaa_env_name}
            if not identical_plugin_files:
                update_request_params['PluginsS3ObjectVersion'] = self._get_object_version(self.s3_plugins_path)
            if not identical_requirement_files:
                update_request_params['RequirementsS3ObjectVersion'] = self._get_object_version(
                    self.s3_requirements_path
                )
            response = mwaa_client.update_environment(**update_request_params)
            logging.info(f'Initiated MWAA update {response}')
        else:
            logging.info(f'Skipped updating MWAA resource as there are no changes')

    def _get_merged_requirements_file_path(self, extra_requirements_files: list = None) -> str:
        """
        Merge main requirements file with extra requirement file
        :param extra_requirements_files: list of requirement file path
        :return: path of merged requirements file
        """
        lines = set()
        requirements_file_path = None
        if self.requirements_path:
            with open(self.requirements_path, 'r') as f:
                contents = f.read()
            lines.update(contents.split('\n'))
        if extra_requirements_files:
            for extra_requirement in extra_requirements_files:
                with open(extra_requirement, 'r') as f:
                    contents = f.read()
                lines.update(contents.split('\n'))
        if lines:
            lines = lines - {''}
            lines = sorted(lines)
            with open(airflow_constants.TMP_REQUIREMENTS_PATH, 'w') as f:
                f.write('\n'.join(lines))
            requirements_file_path = airflow_constants.TMP_REQUIREMENTS_PATH
        return requirements_file_path

    def _get_merged_plugins_zip_path(self, extra_plugins_directory: list = None) -> str:
        """
        Merge main plugins directory with extra plugin directories
        :param extra_plugins_directory: list of plugin directories path
        :return: path of merged zip file
        """
        if extra_plugins_directory is None:
            extra_plugins_directory = []
        if self.plugins_directory:
            extra_plugins_directory.append(self.plugins_directory)
        with zipfile.ZipFile(airflow_constants.TMP_PLUGINS_PATH, 'w') as zipf:
            for plugin_directory in extra_plugins_directory:
                for root, dirs, files in os.walk(plugin_directory):
                    for file in files:
                        zipf.write(
                            os.path.join(root, file),
                            os.path.relpath(os.path.join(root, file), plugin_directory)
                        )
        return airflow_constants.TMP_PLUGINS_PATH if extra_plugins_directory else None

    def _upload_plugins_and_requirements_to_s3(
        self, extra_requirements_files: list = None, extra_plugins_directory: list = None
    ) -> None:
        """
        Zips and upload the plugin directory and requirements file
        :param extra_requirements_files: list of extra requirements file path
        :param extra_plugins_directory: list of extra plugins path
        """
        requirements_file_path = self._get_merged_requirements_file_path(
            extra_requirements_files=extra_requirements_files
        )
        plugins_zip_path = self._get_merged_plugins_zip_path(extra_plugins_directory=extra_plugins_directory)
        if not self.test and self.airflow_stack_name:
            identical_plugin_files = True
            identical_requirement_files = True

            # Upload only if the files on local and s3 are not identical
            if plugins_zip_path and self.s3_plugins_path:
                identical_plugin_files = airflow_utils.upload_file_to_s3(
                    s3_bucket=self.s3_bucket,
                    local_path=plugins_zip_path,
                    s3_path=self.s3_plugins_path
                )

            # Upload only if the files on local and s3 are not identical
            if requirements_file_path and self.s3_requirements_path:
                identical_requirement_files = airflow_utils.upload_file_to_s3(
                    s3_bucket=self.s3_bucket,
                    local_path=requirements_file_path,
                    s3_path=self.s3_requirements_path,
                )

            self._update_mwaa_env(
                identical_plugin_files=identical_plugin_files,
                identical_requirement_files=identical_requirement_files
            )

        else:
            logging.info(
                'Either the test mode is on or airflow stack name is absent. Skipped uploading plugins & requirement'
            )

    def _build_image(
        self, image_name: str, docker_dir: str, docker_tag: str, build_args: dict
    ) -> docker.models.images.Image:
        """
        Builds airflow docker image
        :param image_name: image name
        :param docker_dir: docker image dir where dockerfile is present
        :param docker_tag: image tag
        :param build_args: extra build args for docker image
        :return: build image
        """
        logging.info(f'BUILDING IMAGE: {image_name}:{docker_tag}')
        image, build_log = self.docker_client.images.build(
            path=docker_dir,
            tag=f'{image_name}:{docker_tag}',
            buildargs=build_args
        )
        logging.info(type(image))
        for log in build_log:
            if log.get('stream'):
                logging.info(log.get('stream'))
        return image

    def _tag_and_push_to_ecr(self, image_name: str, image: docker.models.images.Image, tag: str) -> None:
        """
        Tags and push airflow image to ecr
        :param image_name: image name
        :param image: image to push
        :param tag: tag to attach
        """
        logging.info(f'Pushing image to ECR: {image_name}:{tag}')
        ecr_repo_name = f"{self.ecr_registry.replace('https://', '')}/{image_name}"
        image.tag(ecr_repo_name, tag)
        push_log = self.docker_client.images.push(
            ecr_repo_name,
            tag=tag
        )
        if 'errorDetail' in push_log:
            logging.error(push_log)
            raise Exception(push_log)
        logging.info(push_log)

    def process_data(self):
        """
        Process the dags directory to register/update all airflow dags
        """
        projects = list(set(next(walk(self.dags_directory))[1]) - airflow_constants.EXCLUDE_DIRECTORIES)
        for project in projects:
            if (
                    self.project_name_list is None or
                    (self.project_name_list and project in self.project_name_list and not self.exclude) or
                    (self.project_name_list and project not in self.project_name_list and self.exclude)
            ):
                for root, dirs, files in walk(os.path.join(self.dags_directory, project)):
                    for file in files:
                        file_path = path.join(root, file)
                        if (
                                file.endswith('.py') and
                                self.dags_list is None or
                                (
                                        self.dags_list and
                                        file_path in self.dags_list and
                                        not self.exclude
                                ) or
                                (self.dags_list and file_path not in self.dags_list and self.exclude)
                        ):
                            s3_full_path = path.join(self.s3_key, f"{project}_{file_path.split('/')[-1]}")
                            if not self.test:
                                logging.info(f'uploading {file_path} to s3 bucket {s3_full_path}')
                                self.s3_bucket.upload_file(file_path, s3_full_path)
        self._upload_plugins_and_requirements_to_s3()

    def _update_image(
        self, image_name: str, docker_dir: str, docker_tag: str = 'latest', build_args: dict = None
    ) -> None:
        """
        Updates docker image
        :param image_name: image name
        :param docker_dir: docker image dir where dockerfile is present
        :param docker_tag: image tag
        :param build_args: extra build args for docker image
        """
        image = self._build_image(
            image_name=image_name,
            docker_dir=docker_dir,
            build_args=build_args,
            docker_tag=docker_tag
        )
        if not self.test:
            if self.docker_mark_latest:
                self._tag_and_push_to_ecr(image_name=image_name, image=image, tag='latest')
            self._tag_and_push_to_ecr(image_name=image_name, image=image, tag=docker_tag)

    def update_ecs_services(self) -> None:
        """
        Updates all ecs services in given cluster
        """
        ecs_client = boto3.client('ecs')
        ecs_service_arn_list = ecs_client.list_services(cluster=self.ecs_cluster_name).get('serviceArns', [])
        for service_arn in ecs_service_arn_list:
            service_name = service_arn.split(f'{self.ecs_cluster_name}/')[-1]
            if self.ecs_service == airflow_constants.ALL or self.ecs_service in service_name:
                logging.info(f'RESTARTING SERVICE: {service_name}')
                ecs_client.update_service(cluster=self.ecs_cluster_name, service=service_name, forceNewDeployment=True)

    def update_airflow_variables(self) -> None:
        """
        Update airflow variables on MWAA environment
        """
        if not self.mwaa_env_name:
            logging.error(
                f'Airflow environment name absent. Please ensure valid airflow stack name is given.'
            )
            return
        if not os.path.isfile(self.variables_file):
            logging.error(
                f'Cant find a file name {self.variables_file} skipped updating of variables'
            )
            return
        client = boto3.client('mwaa')
        mwaa_cli_token = client.create_cli_token(
            Name=self.mwaa_env_name
        )
        mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
        mwaa_webserver_hostname = f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
        variables_dict = json.load(open(self.variables_file))
        for key, value in variables_dict.items():
            logging.info(f'Setting variable {key}')
            if type(value) is dict:
                value = json.dumps(value).replace('"', '\"')
                data = f'variables set {key} {json.dumps(value)}'
            else:
                data = f'variables set {key} \"{value}\"'
            mwaa_response = requests.post(
                mwaa_webserver_hostname,
                headers={
                    'Authorization': mwaa_auth_token,
                    'Content-Type': 'text/plain'
                },
                data=data
            )
            std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')
            std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
            if std_err_message:
                logging.warning(std_err_message)
            logging.info(std_out_message)
            
    def _copy_dag_to_path(
            self, source_dag_path:str, dag_name:str, project_name: str='common'
        ) -> None:
        """
        copies the dags to local directory
        :param source_dag_path: path to file containing dag
        :param dag_name: name of the dag to copy
        :param project_name: name of project to which the dag belongs to
        """
        os.makedirs(self.local_dag_dir, exist_ok=True)
        new_dag_name = f"{project_name}_{dag_name}"
        local_dag_path = f"{self.local_dag_dir}/{new_dag_name}"
        with open(local_dag_path, "w") as f:
            if not os.path.samefile(source_dag_path, local_dag_path):
                shutil.copyfile(source_dag_path, local_dag_path)

    def _create_dag(self, dag_filepath: str, config_filepath: str, destination_path: str) -> None:
        """
        Create dag from dag template and config file
        :param dag_filepath: dag template file path
        :param config_filepath: config file path
        :param destination_path: destination file path
        """
        with open(config_filepath) as config_file:
            config = json.load(config_file)
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copyfile(dag_filepath, destination_path)
            for line in fileinput.input(destination_path, inplace=True):
                print(line.replace(airflow_constants.CONFIG_PARAMS_MAP_PLACEHOLDER, f'{config}'), end='')

    def process_dynamic_dags(self) -> None:
        """
        Creates and upload dynamic dags to s3 bucket
        """
        projects = airflow_utils.get_dir_file_set_from_path(
            dir_path=self.projects_directory,
            names_list=self.project_name_list,
            exclude=self.exclude,
        )
        # to store all common and project specific requirement file path
        extra_requirements_files = []
        # to store all common and project specific plugins path
        extra_plugins_directory = []

        # upload common dags
        if self.update_common_dags:
            logging.info('Uploading common dags')
            common_dags_list = airflow_utils.get_dir_file_set_from_path(
                dir_path=self.common_dags_directory,
                names_list=self.dags_list,
                exclude=self.exclude,
                fetch_dir=False
            )
            for dag in common_dags_list:
                dag_name = f"common_{dag}"
                dag_file_path = os.path.join(airflow_constants.COMMON_DAGS_DIRECTORY_PATH, dag)
                s3_full_path = os.path.join(self.s3_key, dag_name)
                if self.local_dag_dir:
                    self._copy_dag_to_path(dag_file_path, dag)
                    logging.info(f'Common dag being copied to {self.local_dag_dir}')
                if not self.test:
                    logging.info(f'Uploading common dag to {s3_full_path} in bucket {self.s3_bucket.name}')
                    self.s3_bucket.upload_file(dag_file_path, s3_full_path)
                    logging.info(f'Dag uploaded successfully')
                else:
                    logging.info(f'Common dag upload skipped because of test mode')     
        for project in projects:
            # upload dynamic dags
            if self.update_dynamic_dags:
                dags_template_list = airflow_utils.get_dir_file_set_from_path(
                    dir_path=self.dags_directory,
                    names_list=self.dags_list,
                    exclude=self.exclude,
                    fetch_dir=False
                )
                logging.info(f'Creating dags for project {project}')
                for dag in dags_template_list:
                    dag_name = dag.split('.py')[0]
                    _, _, files = next(
                        walk(os.path.join(self.projects_directory, project, airflow_constants.PARAMETERS, dag_name)),
                        ('', [], [])
                    )
                    for file in files:
                        new_dag_name = f"{project}_{dag_name}_{file.split('.json')[0]}.py"
                        logging.info(f'Generating dag {new_dag_name} from template {dag} and config {file}')
                        tmp_destination_path = os.path.join(airflow_constants.TMP_DAGS_DIR, new_dag_name)
                        self._create_dag(
                            dag_filepath=os.path.join(self.dags_directory, dag),
                            config_filepath=os.path.join(
                                self.projects_directory,
                                project,
                                airflow_constants.PARAMETERS,
                                dag_name,
                                file
                            ),
                            destination_path=tmp_destination_path
                        )
                        s3_full_path = os.path.join(self.s3_key, new_dag_name)
                        logging.info('Dag generated successfully')
                        if self.local_dag_dir:
                            self._copy_dag_to_path(tmp_destination_path, dag_name, project)
                            logging.info(f'Dynamic dag being copied to {self.local_dag_dir}')
                        if not self.test:
                            logging.info(f'Uploading dynamic dag to {s3_full_path} in bucket {self.s3_bucket.name}')
                            self.s3_bucket.upload_file(tmp_destination_path, s3_full_path)
                            logging.info(f'Dag uploaded successfully')
                        else:
                            logging.info(f'Dynamic dag upload skipped because of test mode')
            # Upload standalone dags
            if self.update_standalone_dags:
                logging.info('Uploading standalone dags')
                project_directory_path = os.path.join(self.projects_directory, project, airflow_constants.DAGS_DIRECTORY_PATH)
                standalone_dags_list = airflow_utils.get_dir_file_set_from_path(
                    dir_path=project_directory_path,
                    names_list=self.dags_list,
                    exclude=self.exclude,
                    fetch_dir=False
                )
                for dag in standalone_dags_list:
                    dag_name = f"{project}_{dag}"
                    dag_file_path = os.path.join(project_directory_path, dag_name)
                    s3_full_path = os.path.join(self.s3_key, dag_name)
                    if self.local_dag_dir:
                        self._copy_dag_to_path(os.path.join( project_directory_path, dag), dag, project)
                        logging.info(f'Standalone dag being copied to {self.local_dag_dir}')    
                    if not self.test:
                        logging.info(f'Uploading standalone dag to {s3_full_path} in bucket {self.s3_bucket.name}')
                        self.s3_bucket.upload_file(dag_file_path, s3_full_path)
                        logging.info(f'Dag uploaded successfully')
                    else:
                        logging.info(f'Standalone dag upload skipped because of test mode')
            requirements_file_path = os.path.join(
                self.projects_directory,
                project,
                airflow_constants.REQUIREMENTS_DIR,
                self.requirements_path.split('/')[-1]
            ) if self.requirements_path else ''
            plugins_directory_path = os.path.join(
                self.projects_directory,
                project,
                airflow_constants.PLUGINS_DIRECTORY_PATH
            )
            if os.path.isfile(requirements_file_path):
                extra_requirements_files.append(requirements_file_path)
            if os.path.isdir(plugins_directory_path):
                extra_plugins_directory.append(plugins_directory_path)
        self._upload_plugins_and_requirements_to_s3(
            extra_requirements_files=extra_requirements_files,
            extra_plugins_directory=extra_plugins_directory
        )

    def _get_ecr_repo_name(self, logical_id: str) -> str:
        """
        Return ecr repo name from mdp utils stack
        :param logical_id: logical id of ecr stack
        :return: ecr repo name
        """
        cloudformation_client = self._initialize_cloudformation_client()
        response = cloudformation_client.describe_stack_resource(
            StackName=self.mdp_utils_stack_name,
            LogicalResourceId=logical_id
        )
        return response['StackResourceDetail']['PhysicalResourceId']

    def deploy_task_images(self) -> None:
        """
        Deploys docker images to ecr
        """
        dir_names = set(next(walk(self.docker_file_dir))[1])
        if self.docker_dir_name_list:
            dir_names = dir_names - set(self.docker_dir_name_list) if self.exclude else dir_names.intersection(
                set(self.docker_dir_name_list)
            )
        for dir_name in dir_names:
            aws_config_file = os.path.join(self.docker_file_dir, dir_name, airflow_constants.AWS_DEPLOY_CONFIG)
            if os.path.isfile(aws_config_file):
                logging.info(f'Initiating deployment for project {dir_name} ')
                aws_deploy_dict = json.load(open(aws_config_file))
                ecr_logical_id = aws_deploy_dict.get(airflow_constants.ECR_STACK_LOGICAL_ID)
                if ecr_logical_id:
                    ecr_repo_name = self._get_ecr_repo_name(logical_id=ecr_logical_id)
                    self._update_image(
                        image_name=ecr_repo_name,
                        docker_dir=os.path.join(self.docker_file_dir, dir_name),
                        docker_tag=self.docker_image_tag
                    )
                else:
                    logging.warning(f'No logical id found for {dir_name} skipping deployment')
            else:
                logging.warning(f'No aws_deploy file found for {dir_name} skipping deployment')

    def process(self) -> None:
        """
        Main process function
        """
        if self.mdp_utils_stack_name:
            self.deploy_task_images()
        if self.update_service:
            self.update_ecs_services()
        if self.update_dynamic_dags or self.update_common_dags or self.update_standalone_dags:
            self.process_dynamic_dags()
        elif self.s3_bucket:
            self.process_data()
        if self.variables_file:
            self.update_airflow_variables()
