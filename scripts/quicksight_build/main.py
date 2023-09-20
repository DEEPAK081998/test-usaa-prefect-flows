import argparse
import os

from quicksight_class import QuickSight


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Creates or update the quicksight dataset',
        epilog='When providing --datasets please also specify a single project name  in --projects'
    )
    parser.add_argument('--deploy-datasets', help='deploys dataset on quicksight', action='store_true')
    parser.add_argument(
        '--projects',
        nargs='*',
        default=None,
        help='list of all project names for which datasets needed to be created/updated (This argument is '
             'mandatory when specifying --datasets)'
    )
    parser.add_argument(
        '--datasets',
        nargs='*',
        default=None,
        help='list of all datasets files path which needed to be created/updated '
             '(if this argument is not provided then all datasets will get registered/updated)'
    )
    parser.add_argument(
        '--quicksight-directory',
        type=str,
        help='Directory where all datasets are stored',
        default='quicksight/'
    )
    parser.add_argument(
        '--s3-quicksight-directory',
        type=str,
        help='Directory where all datasets are stored on s3',
        default='quicksight/datasets/'
    )
    parser.add_argument(
        '--s3-bucket-name',
        type=str,
        help='s3 bucket name where all quicksight datasets are stored',
    )
    parser.add_argument(
        '--projects-directory',
        type=str,
        help='Directory where all projects are stored',
        default='projects/'
    )
    parser.add_argument('--exclude', help='exclude the provided dags/project', action='store_true')
    parser.add_argument('--aws-profile', type=str, help='AWS named profile to use')
    args = parser.parse_args()
    return args


def main():
    """
    Main function
    """
    args = _define_arguments()
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    quicksight = QuickSight(**args.__dict__)
    quicksight.run()
    if args.aws_profile:
        del os.environ['AWS_PROFILE']
