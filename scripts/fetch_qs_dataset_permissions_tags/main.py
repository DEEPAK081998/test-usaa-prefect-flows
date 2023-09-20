import boto3
import pandas as pd
import argparse
import json


def fetch_dataset_permissions(profile, account_id, input_file_path):
    # Setup AWS session
    session = boto3.Session(profile_name=profile)
    quicksight_client = session.client('quicksight')

    # Read the input CSV file
    df = pd.read_csv(input_file_path, delimiter=';')

    # New column for Permissions
    df['Permissions'] = None

    for index, row in df.iterrows():
        dataset_id = row['DataSetId']
        try:
            # Retrieve the dataset permissions
            response = quicksight_client.describe_data_set_permissions(
                AwsAccountId=account_id,
                DataSetId=dataset_id
            )
            # Add permissions to the DataFrame
            df.loc[index, 'Permissions'] = json.dumps(response['Permissions'])
        except Exception as e:
            print(f"Error fetching permissions for dataset {dataset_id}: {str(e)}")

    return df


def fetch_dataset_tags(df, profile, account_id, output_file_path):
    # Setup AWS session
    session = boto3.Session(profile_name=profile)
    quicksight_client = session.client('quicksight')

    # New column for Tags
    df['Tags'] = None

    for index, row in df.iterrows():
        dataset_arn = row['Arn']
        try:
            # Retrieve the dataset tags
            response = quicksight_client.list_tags_for_resource(
                ResourceArn=dataset_arn
            )
            # Add tags to the DataFrame
            df.loc[index, 'Tags'] = json.dumps(response['Tags'])
        except Exception as e:
            print(f"Error fetching tags for dataset {dataset_arn}: {str(e)}")

    # Save the DataFrame to the output CSV file
    df.to_csv(output_file_path, index=False)


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Adds column with flag if dataset has refresh schedules or not'
    )
    parser.add_argument('--profile', help='aws profile to use')
    parser.add_argument('--account_id', help='aws account_id to use')
    parser.add_argument('--input_file_path', help='path for dataset describe data csv input file with DataSetId')
    parser.add_argument('--output_file_path', help='path for dataset describe data csv output file '
                                                   'with flag for refresh schedules')

    args = parser.parse_args()
    return args


def main():
    args = _define_arguments()
    df = fetch_dataset_permissions(
        profile=args.profile,
        account_id=args.account_id,
        input_file_path=args.input_file_path
    )
    fetch_dataset_tags(
        df,
        profile=args.profile,
        account_id=args.account_id,
        output_file_path=args.output_file_path
    )
