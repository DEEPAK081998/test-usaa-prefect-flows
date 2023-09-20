import argparse
import os

import constants
import utils
from airflow_class import Airflow


class KeyValue(argparse.Action):
    # class to convert key=value arg to dict
    def __call__(
        self, parser, namespace,
        values, option_string=None
    ):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split('=')
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Creates or update the airbyte connection',
        epilog='When providing --dags please also specify a single project name  in --project'
    )
    parser.add_argument('--test', help='Runs test only Skips dags upload', action='store_true')
    parser.add_argument('--s3-bucket', help='Name of S3 bucket where to upload the dags')
    parser.add_argument('--s3-key', help='Path is S3 bucket where to store the dags', default='airflow/dags/')
    parser.add_argument(
        '--requirements-file',
        help='Path to the requirements.txt file for airflow',
        default='airflow/common/requirements/airflow_requirements.txt'
    )
    parser.add_argument(
        '--s3-requirements-path',
        help='Full path in S3 bucket where to store the requirements file',
        default='airflow/airflow_requirements.txt'
    )
    parser.add_argument(
        '--plugins-directory',
        help='Directory where all plugins are stored',
        default='airflow/common/plugins/'
    )
    parser.add_argument(
        '--s3-plugins-path',
        help='Full path in S3 bucket where to store the pulgins zip file',
        default='airflow/airflow_dags_plugins.zip'
    )
    parser.add_argument(
        '--projects',
        nargs='*',
        default=None,
        help='list of all project names for which dags needed to be created/updated (This argument is '
             'mandatory when specifying --dags)'
    )
    parser.add_argument(
        '--dags',
        nargs='*',
        default=None,
        help='list of all dags files path which needed to be created/updated '
             '(if this argument is not provided then all dags will get registered/updated)'
    )
    parser.add_argument(
        '--dags-directory',
        type=str,
        help='Directory where all dags (or templates in case of dynamic dags) are stored',
        default='airflow/dags_template/'
    )
    parser.add_argument('--airflow-stack-name', type=str, help='Name of airflow stack', default=None)
    parser.add_argument('--exclude', help='exclude the provided dags/project', action='store_true')
    parser.add_argument('--aws-profile', type=str, help='AWS named profile to use')
    parser.add_argument('--project-name', type=str, help='project name for which to build image', default=None)
    parser.add_argument(
        '--docker-image-tag',
        type=str,
        help='docker image tag to assign(random 16 bit hash will be assigned otherwise)',
        default=utils.generate_hash(16)
    )
    parser.add_argument(
        '--not-mark-latest',
        help='will not mark the docker image as latest on ECR',
        action='store_true'
    )
    parser.add_argument(
        '--docker-build-args',
        nargs='*',
        help='Any additional build args needed for image in form of KEY=VALUE format',
        action=KeyValue
    )
    parser.add_argument(
        '--docker-file-dir',
        type=str,
        help='dir where docker file is present',
        default='airflow/'
    )
    parser.add_argument(
        '--update-ecs-services',
        help='whether to update ecs services or not',
        action='store_true'
    )
    parser.add_argument(
        '--ecs-cluster-name',
        type=str,
        help='ecs cluster name where airflow is deployed',
        default=None
    )
    parser.add_argument(
        '--update-variables',
        type=str,
        help='path to variables files that needs to be updated',
        default=None
    )
    parser.add_argument('--mdp-utils-stack-name', type=str, help='Name of mdp utils stack', default=None)
    parser.add_argument(
        '--docker-dir-names',
        nargs='*',
        default=None,
        help='list of all docker dirs names which needs to be included'
    )
    parser.add_argument('--ecs-service', type=str, choices=constants.ECS_SERVICE_CHOICE, default='all')
    parser.add_argument(
        '--projects-directory',
        type=str,
        help='Directory where all projects are stored',
        default='airflow/projects'
    )
    parser.add_argument(
        '--update-dynamic-dags',
        help='whether to update dynamic dags or not',
        action='store_true'
    )
    parser.add_argument(
        '--update-common-dags',
        help='whether to update common dags or not',
        action='store_true'
    )
    parser.add_argument(
        '--common-dags-directory',
        type=str,
        help='Directory where all common dags are stored',
        default='airflow/common/dags'
    )
    parser.add_argument(
        '--update-standalone-dags',
        help='whether to update standalone project dags or not',
        action='store_true'
    )
    parser.add_argument(
        '--airflow-resource-logical-id',
        type=str,
        help='Logical id of airflow resource provided in cloudformation template',
    )
    parser.add_argument(
        '--local-dag-dir',
        type=str,
        help="Path to the directory where you want to store the local dags",
    )
    args = parser.parse_args()
    if args.dags and not args.projects and not args.exclude:
        raise ValueError(
            '--dags argument also need project name when exclude is not set, please specify'
            ' a project in --project argument'
        )
    return args


def main():
    """
    Main function
    """
    args = _define_arguments()
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    airflow = Airflow(
        s3_bucket=args.s3_bucket,
        s3_key=args.s3_key,
        projects=args.projects,
        dags=args.dags,
        dags_directory=args.dags_directory,
        test=args.test,
        exclude=args.exclude,
        requirements_file=args.requirements_file,
        s3_requirements_path=args.s3_requirements_path,
        plugins_directory=args.plugins_directory,
        s3_plugins_path=args.s3_plugins_path,
        airflow_stack_name=args.airflow_stack_name,
        docker_image_tag=args.docker_image_tag,
        docker_file_dir=args.docker_file_dir,
        not_mark_latest=args.not_mark_latest,
        docker_build_args=args.docker_build_args,
        ecs_cluster_name=args.ecs_cluster_name,
        ecs_service=args.ecs_service,
        update_ecs_services=args.update_ecs_services,
        update_variables=args.update_variables,
        projects_directory=args.projects_directory,
        update_dynamic_dags=args.update_dynamic_dags,
        update_common_dags=args.update_common_dags,
        common_dags_directory=args.common_dags_directory,
        update_standalone_dags=args.update_standalone_dags,
        mdp_utils_stack_name=args.mdp_utils_stack_name,
        docker_dir_names=args.docker_dir_names,
        airflow_resource_logical_id=args.airflow_resource_logical_id,
        local_dag_dir=args.local_dag_dir,
    )
    airflow.process()
    if args.aws_profile:
        del os.environ['AWS_PROFILE']
