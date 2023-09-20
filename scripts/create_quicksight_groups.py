import http
import json
import logging
import os
from os import walk
from typing import Any

import boto3
import argparse
import os


logging.basicConfig(level=logging.INFO)


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Creates or update the quicksight groups',
        epilog=''
    )
    parser.add_argument('--aws-profile', type=str, help='AWS named profile to use')
    args = parser.parse_args()
    return args

def create_groups(group, group_desc):
    client = boto3.client('quicksight')
    aws_account_id = 931422288647

    create = False
    try:
        response = client.describe_group(GroupName=group,
                                         AwsAccountId=aws_account_id,
                                         Namespace='default'
                                         )
        logging.info(f'The given group {group} for account {aws_account_id} does exists')
        logging.info(f'{response}')
    except client.exceptions.ResourceNotFoundException as e:
        logging.info(f'The given group {group} for account {aws_account_id} does not exists and it would be created')
        create = True

    if create:
        logging.info(f'Creating group {group} in account {aws_account_id}')
        response = client.create_group(GroupName=group,
                                       Description=group_desc,
                                       AwsAccountId=aws_account_id,
                                       Namespace='default'
                                       )
        if response['Status'] == http.HTTPStatus.CREATED:
            logging.info(f'Group successfully successfully')
            logging.info(f'{response}}')


def main():
    """
    Main function
    """
    args = _define_arguments()
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    create_groups('Reporters','power to create, modify, and delete a dataset')
    create_groups('Reader','Read Only Group')
    if args.aws_profile:
        del os.environ['AWS_PROFILE']

if __name__ == '__main__':
    main()
