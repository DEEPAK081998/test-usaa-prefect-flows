import argparse
import os
import json
import pandas as pd
import ast


# Define transformation functions

def add_prefix_to_string(id_string, prefix='SNF-'):
    """Adds a prefix to a string."""
    return f"{prefix}{id_string}"


def uppercase_resources(resource):
    """Recursively traverse a dictionary (or other data structure) and uppercase all string values."""
    if isinstance(resource, str):
        return resource.upper()
    elif isinstance(resource, dict):
        return {k: uppercase_resources(v) for k, v in resource.items()}
    elif isinstance(resource, list):
        return [uppercase_resources(v) for v in resource]
    else:
        return resource


def load_json(json_str):
    """Loads a JSON object from a string, handling single quotes."""
    python_obj = ast.literal_eval(json_str)
    json_str_double_quotes = json.dumps(python_obj)
    return json.loads(json_str_double_quotes)


def set_has_custom_sql_flag(row):
    # Load the 'PhysicalTableMap' as a Python object
    physical_table_map = load_json(row['PhysicalTableMap'])

    has_custom_sql = False

    # Iterate over each entry in the 'PhysicalTableMap'
    for id, entry in list(physical_table_map.items()):
        # use list() to avoid RuntimeError: dictionary changed size during iteration
        if 'CustomSql' in entry:
            has_custom_sql = True

    return has_custom_sql


def transform_physical_table_map(row, new_data_source_arn="new_snowflake_arn", output_dir="."):
    # Load the 'PhysicalTableMap' as a Python object
    physical_table_map = load_json(row['PhysicalTableMap'])

    # Iterate over each entry in the 'PhysicalTableMap'
    for id, entry in list(physical_table_map.items()):
        # use list() to avoid RuntimeError: dictionary changed size during iteration

        # Add the 'SNF-' prefix to the id
        new_id = add_prefix_to_string(id)

        # Replace the old id with the new one in the 'PhysicalTableMap'
        physical_table_map[new_id] = physical_table_map.pop(id)

        if 'CustomSql' in entry:
            # Save the SQL query to a file
            sql_file_name = f"{row['Name'].replace('/', '_')}_{entry['CustomSql']['Name'].replace('/', '_').replace(' ', '_').upper()}.sql"
            with open(os.path.join(output_dir, sql_file_name), 'w') as f:
                f.write(entry['CustomSql']['SqlQuery'])

            # Replace the 'CustomSql' with 'RelationalTable'
            entry['RelationalTable'] = {}
            entry['RelationalTable']['DataSourceArn'] = new_data_source_arn
            entry['RelationalTable']['Name'] = sql_file_name[:-4].upper()  # Remove the ".sql" extension
            entry['RelationalTable']['Schema'] = 'PUBLIC'
            entry['RelationalTable']['InputColumns'] = uppercase_resources(entry['CustomSql']['Columns'])
            del entry['CustomSql']

        elif 'RelationalTable' in entry:
            entry['RelationalTable']['DataSourceArn'] = new_data_source_arn
            # Uppercase the 'Name', 'Schema', and 'InputColumns' fields
            entry['RelationalTable']['Name'] = uppercase_resources(entry['RelationalTable']['Name'])
            entry['RelationalTable']['Schema'] = uppercase_resources(entry['RelationalTable']['Schema'])
            entry['RelationalTable']['InputColumns'] = uppercase_resources(entry['RelationalTable']['InputColumns'])

    return physical_table_map


def transform_logical_table_map(row, df):
    """Transforms the 'LogicalTableMap' column of a dataframe row."""
    # Load the 'LogicalTableMap' as a Python object
    logical_table_map = load_json(row['LogicalTableMap'])

    # Load the 'PhysicalTableMap' as a Python object
    physical_table_map = row['PhysicalTableMap']

    # Iterate over each entry in the 'LogicalTableMap'
    for id, entry in list(logical_table_map.items()):  # use list() to avoid RuntimeError: dictionary changed size during iteration
        # Add the 'SNF-' prefix to the id
        new_id = add_prefix_to_string(id)

        # Replace the old id with the new one in the 'LogicalTableMap'
        logical_table_map[new_id] = logical_table_map.pop(id)

        source = entry['Source']

        # Handle 'PhysicalTableId' in 'Source'
        if 'PhysicalTableId' in source:
            # Add the 'SNF-' prefix to the 'PhysicalTableId'
            source['PhysicalTableId'] = add_prefix_to_string(source['PhysicalTableId'])

            # Add 'RenameColumnOperation' to the beginning of 'DataTransforms' for each 'InputColumn' in the physical table
            physical_table = physical_table_map[source['PhysicalTableId']]
            input_columns = physical_table['RelationalTable']['InputColumns']
            for column in input_columns:
                rename_column_operation = {
                    "RenameColumnOperation": {
                        "ColumnName": column['Name'],
                        "NewColumnName": column['Name'].lower()
                    }
                }
                if 'DataTransforms' not in entry:
                    entry['DataTransforms'] = []
                entry['DataTransforms'].insert(0, rename_column_operation)

        # Handle 'DataSetArn' in 'Source'
        if 'DataSetArn' in source:
            dataset_id = source['DataSetArn'].split('/')[-1]
            prefixed_dataset_id = add_prefix_to_string(dataset_id)
            if prefixed_dataset_id in df['DataSetId'].values:
                # If the dataset id is in the CSV file, add the 'SNF-' prefix to it
                source['DataSetArn'] = source['DataSetArn'].replace(dataset_id, add_prefix_to_string(dataset_id))

        # Handle 'JoinInstruction' in 'Source'
        if 'JoinInstruction' in source:
            join_instruction = source['JoinInstruction']
            # Add the 'SNF-' prefix to the 'LeftOperand' and 'RightOperand'
            join_instruction['LeftOperand'] = add_prefix_to_string(join_instruction['LeftOperand'])
            join_instruction['RightOperand'] = add_prefix_to_string(join_instruction['RightOperand'])

        # Handle 'CreateColumnsOperation' in 'DataTransforms'
        if 'DataTransforms' in entry:
            for transform in entry['DataTransforms']:
                if 'CreateColumnsOperation' in transform:
                    for column in transform['CreateColumnsOperation']['Columns']:
                        # Add the 'SNF-' prefix to the 'ColumnId'
                        column['ColumnId'] = add_prefix_to_string(column['ColumnId'])

    return logical_table_map


def transform_name(row, prefix='SNF-'):
    """Adds a prefix to a name."""
    name = row['Name']
    if isinstance(name, str):
        name = name.replace(' ', '_').replace('/', '_')
    return add_prefix_to_string(name, prefix)


def transform_dataset_id(row, prefix='SNF-'):
    """Adds a prefix to a name."""
    dataset_id = row['DataSetId']
    return add_prefix_to_string(dataset_id, prefix)


def migrate_datasets(new_data_source_arn, input_csv_path, output_path, aws_account_id):
    # Load the input CSV file into a pandas DataFrame
    df = pd.read_csv(input_csv_path, delimiter=';')

    # Make sure the output directory exists
    sql_files_output_path = os.path.join(output_path, 'dbt_models')
    custom_sql_datasets_output_path = os.path.join(output_path, 'custom_sql_datasets')

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(sql_files_output_path, exist_ok=True)
    os.makedirs(custom_sql_datasets_output_path, exist_ok=True)

    # New column for HasCustomSql
    df['HasCustomSql'] = False

    # Define a mapping from column names to transformation functions
    transformation_functions = {
        'DataSetId': transform_dataset_id,
        'Name': transform_name,
        'HasCustomSql': set_has_custom_sql_flag,
        'PhysicalTableMap': lambda r: transform_physical_table_map(r, new_data_source_arn,
                                                                   output_dir=sql_files_output_path),
        'LogicalTableMap': lambda r: transform_logical_table_map(r, df)
    }

    # Apply the transformation functions to each row
    for column, func in transformation_functions.items():
        df[column] = df.apply(lambda row: func(row), axis=1)

    # Add the 'AwsAccountId' field to each row
    df['AwsAccountId'] = aws_account_id

    # Make 'AwsAccountId' the first column in the DataFrame
    ordered_columns = ['AwsAccountId'] + [c for c in df.columns if c != 'AwsAccountId']
    df = df[ordered_columns]

    # Convert the 'DataSetUsageConfiguration' column to a JSON object
    df['DataSetUsageConfiguration'] = df['DataSetUsageConfiguration'].apply(load_json)
    # Convert the 'Permissions' column to a JSON object
    df['Permissions'] = df['Permissions'].apply(load_json)

    # Save each row of the transformed DataFrame as a JSON file in the output directory
    for _, row in df.iterrows():
        # Determine the appropriate directory based on the 'HasCustomSql' flag
        if row['HasCustomSql']:
            output_dir = custom_sql_datasets_output_path
        else:
            output_dir = output_path

        # Columns to exclude from the output JSON files
        columns_to_exclude = ['Arn', 'CreatedTime', 'LastUpdatedTime', 'OutputColumns',
                              'ConsumedSpiceCapacityInBytes', 'DataSourceArn', 'HasRefreshSchedule',
                              'ColumnGroups', 'FieldFolders', 'RowLevelPermissionDataSet', 'DatasetParameters',
                              'Tags', 'HasCustomSql']

        if pd.notnull(row['ColumnGroups']):
            columns_to_exclude.remove('ColumnGroups')
            # Convert the 'ColumnGroups' column to a JSON object
            row['ColumnGroups'] = load_json(row['ColumnGroups'])

        # Create a dictionary from the row, exclude not necessary columns
        row_dict = row.drop(columns_to_exclude).to_dict()

        # Save the row as a JSON file in the determined directory
        with open(os.path.join(output_dir, f"{row['Name']}.json"), 'w') as f:
            json.dump(row_dict, f)


def _define_arguments():
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(
        description='Creates quicksight dataset json files for create or update operations'
    )
    parser.add_argument('--source-csv-path', help='path of csv file which contains description for existing datasets')
    parser.add_argument('--destination-dir-path', help='destination directory path for new dataset json files')
    parser.add_argument('--new-data-source-arn', help='new data source arn which will be used in new datasets')
    parser.add_argument('--aws_account_id', help='aws account id')

    args = parser.parse_args()
    return args


def main():
    """
    Main function
    """
    args = _define_arguments()
    migrate_datasets(
        new_data_source_arn=args.new_data_source_arn,
        input_csv_path=args.source_csv_path,
        output_path=args.destination_dir_path,
        aws_account_id=args.aws_account_id
    )

