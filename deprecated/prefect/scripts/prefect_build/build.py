"""
Script to Builds and register all prefect flows

Usage:
python build.py [-h] [--test] [--debug] [--labels [LABELS [LABELS ...]]]
                [--ecs-image ECS_IMAGE] [--s3-bucket S3_BUCKET]
                [--flow-name-prefix FLOW_NAME_PREFIX]
                [--flow-name-suffix FLOW_NAME_SUFFIX]
                [--aws-profile AWS_PROFILE] [--flows [FLOWS [FLOWS ...]] |
                --projects [PROJECTS [PROJECTS ...]]] [--exclude]

optional arguments:
  -h, --help            show this help message and exit
  --test                Run tests only. Skips registration
  --debug               Include debug logs
  --labels [LABELS [LABELS ...]]
                        Labels to provide to ECS agent
  --ecs-image ECS_IMAGE
                        Image to provide to prefect ECS agent
  --s3-bucket S3_BUCKET
                        AWS S3 bucket for storing flows
  --flow-name-prefix FLOW_NAME_PREFIX
                        suffix to add to all flow names
  --flow-name-suffix FLOW_NAME_SUFFIX
                        prefix to add to all flow names
  --aws-profile AWS_PROFILE
                        name of the profile to use
  --flows [FLOWS [FLOWS ...]]
                        list of all flows path which needed to be
                        registered/updated (if this argument is not provided
                        then all flows will get registered/updated)
  --projects [PROJECTS [PROJECTS ...]]
                        list of all project names for which flows needed to be
                        registered/updated (if this argument is not provided
                        then flows in all projects will get
                        registered/updated)
  --exclude             exclude the provided flows

please provide either --flows or --project but not both at the same time
"""

import argparse
import logging
import os
from collections import defaultdict
from os import path, walk

from prefect.utilities.storage import extract_flow_from_file

import build_config


def _get_flows(project_list: [str] = None, flows_path: [str] = None, exclude: bool = False) -> defaultdict:
    """
    Fetch all flows in the flows directory
    :param project_list: list of all project names
    :param flows_path: list of flow path
    :param exclude: whether to exclude or not
    :return: dict contain the project name as key and list of found flows as value
    """
    logging.info('Discovering flows...')
    flows_dict = defaultdict(list)
    flows_dir = 'flows'
    ignore_paths = ['__pycache__', 'schemas', 'files', 'sql', 'flows']
    projects = next(walk(flows_dir))[1]
    for project in projects:
        if (
                project_list is None or
                (project_list and project in project_list and not exclude) or
                (project_list and project not in project_list and exclude)
        ):
            logging.debug(f'registering flows for {project}')
            for root, dirs, files in walk(path.join(flows_dir, project)):
                if path.basename(root) not in ignore_paths and path.basename(root) != project:
                    file_path = path.join(root, 'flow.py')
                    logging.debug(f'flow path:- {file_path}')
                    if (
                            flows_path is None or
                            (flows_path and file_path in flows_path and not exclude) or
                            (flows_path and file_path not in flows_path and exclude)
                    ):
                        try:
                            flow = extract_flow_from_file(file_path)
                            logging.info(f'Found flow {flow.name}')
                            flows_dict[project].append(flow)
                        except Exception as e:
                            logging.error(e)
    return flows_dict


def _build_flows(
    flows_dict: defaultdict, labels: [str] = None, bucket: str = None, test_mode=False, flow_name_suffix: str = '',
    flow_name_prefix: str = '', ecs_image: str = None
):
    """
    Build all the flows presented in flows_dict
    :param flows_dict: dict contain the project name as key and list of found flows as value
    :param labels: list of labels to attach to ecs agent
    :param bucket: s3 bucket name
    :param test_mode: indicate whether testing the flows or actually registering them
    :param flow_name_suffix: additional name to prepend to flow name
    :param flow_name_prefix:  additional name to append to flow name
    :param ecs_image: any image to pass to ecs run task
    """
    for project, flows in flows_dict.items():
        logging.debug(f'project: {project}')
        for flow in flows:
            flow.name = flow_name_prefix + flow.name + flow_name_suffix
            logging.debug(flow.name)
            flow.validate()
            if not flow.run_config:
                flow.run_config = build_config.get_default_run_config(labels=labels, image=ecs_image)
            if not flow.storage:
                flow.storage = build_config.get_default_storage(bucket=bucket)
            if not test_mode:
                flow.register(
                    project_name=project,
                    idempotency_key=flow.serialized_hash(),
                )


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Builds and register all prefect flows',
        epilog='please provide either --flows or --project but not both at the same time'
    )
    parser.add_argument('--test', help='Run tests only. Skips registration', action='store_true')
    parser.add_argument('--debug', help='Include debug logs', action='store_true')
    parser.add_argument('--labels', nargs='*', default=None, help='Labels to provide to ECS agent')
    parser.add_argument('--ecs-image', type=str, default=None, help='Image to provide to prefect ECS agent')
    parser.add_argument('--s3-bucket', type=str, help='AWS S3 bucket for storing flows')
    parser.add_argument('--flow-name-prefix', type=str, default='', help='suffix to add to all flow names')
    parser.add_argument('--flow-name-suffix', type=str, default='', help='prefix to add to all flow names')
    parser.add_argument('--aws-profile', type=str, help='name of the profile to use')
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--flows',
        nargs='*',
        default=None,
        help='list of all flows path which needed to be registered/updated (if this argument is not provided then '
             'all flows will get registered/updated)'
    )
    group.add_argument(
        '--projects',
        nargs='*',
        default=None,
        help='list of all project names for which flows needed to be registered/updated (if this argument '
             'is not provided then flows in all projects will get registered/updated)'
    )
    parser.add_argument('--exclude', help='exclude the provided flows ', action='store_true')
    return parser.parse_args()


def main():
    args = _define_arguments()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    logging.getLogger('prefect').setLevel(level=logging.ERROR)
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    flows_dict = _get_flows(project_list=args.projects, flows_path=args.flows, exclude=args.exclude)
    _build_flows(
        flows_dict,
        test_mode=args.test,
        labels=args.labels,
        bucket=args.s3_bucket,
        flow_name_prefix=args.flow_name_prefix,
        flow_name_suffix=args.flow_name_suffix,
        ecs_image=args.ecs_image
    )
    if args.aws_profile:
        del os.environ['AWS_PROFILE']


if __name__ == '__main__':
    main()
