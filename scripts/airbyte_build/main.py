import argparse
import os

from airbyte import AirbyteConnection, AirbyteProcessor


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Creates or update the airbyte connection',
        epilog='When providing --connections please also specify a single project name  in --project'
    )
    parser.add_argument('--airbyte-api-url', type=str, help='Airbyte API url')
    parser.add_argument(
        '--airbyte-secrets-env-name',
        type=str,
        help='Airbyte Secrets Environment Variable Name',
        default=''
    )
    parser.add_argument('--workspace', type=str, help='Workspace ID')
    parser.add_argument(
        '--projects',
        nargs='*',
        default=None,
        help='list of all project names for which connections needed to be created/updated (This argument is '
             'mandatory when specifying --connections)'
    )
    parser.add_argument(
        '--connections',
        nargs='*',
        default=None,
        help='list of all connection files path which needed to be created/updated '
             '(if this argument is not provided then all connections will get registered/updated)'
    )
    parser.add_argument(
        '--connection-directory',
        type=str,
        help='Directory where all connections are stored',
        default='connection_config/'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--table-name',
        type=str,
        help='DynamoDb table name where to store connection data',
    )
    group.add_argument('--test', help='Runs test only Skips dynamodb connection', action='store_true')
    group.add_argument('--zip-file-path', type=str, help='local or s3 path where to store state zip file')
    parser.add_argument(
        '--env-file-path',
        type=str,
        help='env file path',
        default=f"{os.path.expanduser('~')}/.octavia"
    )
    parser.add_argument('--exclude', help='exclude the provided connections/project ', action='store_true')
    parser.add_argument('--use-first-workspace', help='user first fetched workspace', action='store_true')
    parser.add_argument(
        '--store-connection',
        help='Store connection data in file instead of putting them to airbyte',
        action='store_true'
    )
    parser.add_argument('--aws-profile', type=str, help='AWS named profile to use')
    parser.add_argument('--add-id-to-file', help='add id in file name', action='store_true')
    parser.add_argument(
        '--octavia-deploy',
        help='deploy airbyte connections through octavia',
        action='store_true'
    )
    parser.add_argument(
        '--airbyte-directory',
        type=str,
        help='Directory where all airbyte related files are stored',
        default='airbyte/'
    )
    parser.add_argument(
        '--projects-directory',
        type=str,
        help='Directory where all projects are stored',
        default='projects/'
    )
    parser.add_argument(
        '--source-directory',
        type=str,
        help='Project source directory',
        default=os.getcwd()
    )
    parser.add_argument(
        '--load-state-files',
        help='only loads state file',
        action='store_true'
    )
    parser.add_argument(
        '--upload-state-files',
        help='only uploads state file',
        action='store_true'
    )
    parser.add_argument(
        '--api-header-file-path',
        type=str,
        help='api header file name(it should be present in your connections directory',
        default=f"api_http_headers.yaml"
    )
    args = parser.parse_args()
    if args.connections and not args.projects and not args.exclude:
        raise ValueError(
            '--connections argument also need project name when exclude is not set, please specify'
            ' a project in --project argument'
        )
    if args.octavia_deploy and not args.airbyte_api_url:
        raise ValueError(
            '--octavia_deploy argument also need airbyte url, please specify url in --airbyte-api-url argument'
        )
    return args


def main():
    """
    Main function
    """
    args = _define_arguments()
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    if args.octavia_deploy or args.load_state_files or args.upload_state_files:
        AirbyteProcessor(
            airbyte_api_url=args.airbyte_api_url,
            env_file_path=args.env_file_path,
            zip_file_path=args.zip_file_path,
            airbyte_secrets_env_name=args.airbyte_secrets_env_name,
            airbyte_directory=args.airbyte_directory,
            projects_directory=args.projects_directory,
            connection_directory=args.connection_directory,
            source_directory=args.source_directory,
            projects=args.projects,
            exclude=args.exclude,
            test=args.test,
            only_load_state_files=args.load_state_files,
            only_upload_state_files=args.upload_state_files,
            api_header_file_path=args.api_header_file_path

        ).run()
    else:
        AirbyteConnection(
            connection_directory=args.connection_directory,
            connection_list=args.connections,
            project_names=args.projects,
            exclude=args.exclude,
            workspace_id=args.workspace,
            use_first_workspace=args.use_first_workspace,
            table_name=args.table_name,
            test=args.test,
            airbyte_secrets_env_name=args.airbyte_secrets_env_name,
            api_base_url=args.airbyte_api_url,
            store_connection=args.store_connection,
            add_id_to_file=args.add_id_to_file
        ).process_data()
    if args.aws_profile:
        del os.environ['AWS_PROFILE']
