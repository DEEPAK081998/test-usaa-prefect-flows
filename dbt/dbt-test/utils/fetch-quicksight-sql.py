import boto3
import botocore.client
from datetime import datetime
import argparse


def parse_args() -> argparse.Namespace:
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", nargs="+",
                        help="AWS region to explore", type=str, required=True)
    parser.add_argument("--profile", nargs="+",
                        help="AWS profile to use", type=str, required=True)
    parser.add_argument("--pageSize", help="Results per page",
                        type=int, required=False, default=100)
    parser.add_argument("--maxItems", help="Maximum total results",
                        type=int, required=False, default=None)
    return parser.parse_args()


def format_date(d: datetime) -> str:
    """
    Format date
    :param d: Datetime object
    :return: (string) Date string  
    """
    return d.strftime("%x")


def fetch_data_sets(client: botocore.client.BaseClient, accountId: str, resultsPerPage: int, maxItems: int = None) -> list:
    """
    Fetch data sets from quicksight.
    :param client: AWS boto3 client object
    :param accountId: Account Id
    :param resultsPerPage: results per page
    :param maxItems: max results
    :return: (list) list of data sets
    """
    paginator = client.get_paginator('list_data_sets')
    data_set_iterator = paginator.paginate(
        AwsAccountId=accountId,
        PaginationConfig={
            'MaxItems': maxItems,
            'PageSize': resultsPerPage,
        }
    )

    data_sets = []
    for data_set in data_set_iterator:
        data_sets = data_sets + data_set['DataSetSummaries']
    return data_sets


if __name__ == '__main__':
    args = parse_args()
    pageSize = args.pageSize
    maxItems = args.maxItems
    for profile in args.profile:
        session = boto3.Session(profile_name=profile)

        for region in args.region:
            quicksight = session.client('quicksight', region_name=region)
            data_sets = fetch_data_sets(
                quicksight, '931422288647', pageSize, maxItems)
            for data_set_summary in data_sets:
                data_set_id = data_set_summary['DataSetId']
                try:
                    data_set = quicksight.describe_data_set(
                        AwsAccountId='931422288647', DataSetId=data_set_id)
                    physical_map = data_set.get(
                        'DataSet', {}).get('PhysicalTableMap', {})
                    temp = list(physical_map.values())[0]
                    # hope_sql = next(iter(physical_map))
                    if temp:
                        custom_sql = temp.get('CustomSql')
                        if custom_sql:
                            print(custom_sql.get('Name'))
                            print(custom_sql.get('SqlQuery'))
                            print('-----------------------------------------')
                except Exception:
                    pass
