import argparse
import os

from workflow_common_util import WorkflowCommonUtil


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(description='Common util script for all workflow resources')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--test', help='Runs test only skips project upload', action='store_true')
    group.add_argument('--s3-bucket', help='Name of S3 bucket where to upload the workflow projects')
    parser.add_argument(
        '--s3-path',
        help='Path inside bucket where to upload the workflow projects (should point to a directory)',
    )
    parser.add_argument('--aws-profile', type=str, help='AWS named profile to use')
    parser.add_argument(
        '--projects-folder-path',
        type=str, help='Path of the directory that has the projects',
        required=True
    )
    parser.add_argument(
        '--projects',
        nargs='*',
        default=None,
        help='List of all wprkflow project names needed to be zipped and uploaded'
    )
    parser.add_argument('--exclude', help='Exclude the provided workflow projects', action='store_true')
    args = parser.parse_args()
    return args


def main():
    """
    Main function
    """
    args = _define_arguments()
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    WorkflowCommonUtil(
        s3_path=args.s3_path,
        s3_bucket_name=args.s3_bucket,
        exclude=args.exclude,
        projects_folder_path=args.projects_folder_path,
        project_names=args.projects,
        test=args.test,
    ).process_data()
    if args.aws_profile:
        del os.environ['AWS_PROFILE']
