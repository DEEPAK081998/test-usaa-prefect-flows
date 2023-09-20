import pandas as pd
import argparse
from datetime import datetime

# Define a recursive function to search for 'DataSourceArn'
def find_data_source_arn(data):
    if isinstance(data, dict):
        if 'DataSourceArn' in data:
            return data['DataSourceArn']
        for value in data.values():
            result = find_data_source_arn(value)
            if result:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_data_source_arn(item)
            if result:
                return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to extract data from a CSV file and create a new CSV file.")
    parser.add_argument("input_file", help="Path to the input CSV file.")
    args = parser.parse_args()

    df = pd.read_csv(args.input_file)

    # Apply the function to extract 'DataSourceArn' and create a new column
    df['DataSourceArn'] = df['PhysicalTableMap'].apply(lambda x: find_data_source_arn(eval(x)))
    final = df[df['DataSourceArn'] == 'arn:aws:quicksight:us-east-1:931422288647:datasource/53683626-4527-4f33-a36d-f7cf91a9c44d']

    # Add the current date to the output filename
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_filename = f'hs_dw_qs_datasets_{current_date}.csv'

    final.reset_index(drop=True, inplace=True)
    final.to_csv(output_filename, index=False)

    print(f"Data extraction complete. Output saved to '{output_filename}'.")

# example command: python datasource_filter.py datasetdescribe.csv
