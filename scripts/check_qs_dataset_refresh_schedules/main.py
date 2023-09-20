import boto3
import pandas as pd
import argparse


def check_dataset_refresh_schedule(profile, account_id, input_file_path, output_file_path):
    # Setup AWS session
    session = boto3.Session(profile_name=profile)
    quicksight_client = session.client('quicksight')

    # Read the input CSV file
    df = pd.read_csv(input_file_path, delimiter=';')

    # New column for refresh schedule flag
    df['HasRefreshSchedule'] = False

    for index, row in df.iterrows():
        dataset_id = row['DataSetId']
        try:
            # Retrieve the dataset refresh schedules
            response = quicksight_client.list_refresh_schedules(
                AwsAccountId=account_id,
                DataSetId=dataset_id
            )
            # Check if the dataset has a refresh schedule
            if response['RefreshSchedules']:
                df.loc[index, 'HasRefreshSchedule'] = True
        except Exception as e:
            print(f"Error checking dataset {dataset_id}: {str(e)}")

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
    """
    Main function
    """
    args = _define_arguments()
    check_dataset_refresh_schedule(
        profile=args.profile,
        account_id=args.account_id,
        input_file_path=args.input_file_path,
        output_file_path=args.output_file_path
    )
