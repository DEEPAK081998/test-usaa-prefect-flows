"""
A Script to create dbt models
Usage:
create_models.py [-h] --target TARGET --ddl-file DDL_FILE
                        [--sources-dir SOURCES_DIR]

optional arguments:
  -h, --help            show this help message and exit
  --target TARGET       path to models folder
  --ddl-file DDL_FILE   postgres ddl file
  --sources-dir SOURCES_DIR
                        directory where to create sources file

"""
import argparse
import os.path
import typing
from collections import defaultdict
from typing import Tuple

import yaml


def _define_arguments() -> argparse.Namespace:
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(description='Script to create dbt models')
    parser.add_argument('--target', type=str, help='path to models folder', required=True)
    parser.add_argument('--ddl-file', type=str, help='postgres ddl file', required=True)
    parser.add_argument('--sources-dir', type=str, help='directory where to create sources file')
    return parser.parse_args()


def _get_column(line: str) -> str:
    """
    Return the column that needed to be inserted in sql file with any type cast
    :param line: file line
    :return: modified string that needed to be added in file
    """
    column_name = line.split()[0]
    if 'uuid' in line:
        return f'\t cast({column_name} as uuid)'
    elif 'timestamp without time zone' in line:
        return f'\t cast({column_name} as timestamp without time zone)'
    elif 'json' in line:
        return f'\t cast({column_name} as json)'
    elif 'bigint' in line and 'id' in line:
        return f'\t cast({column_name} as bigint)'
    return f'\t {column_name}'


def _get_table_info_from_prefix(splitted_line: list, prefix_word: str) -> Tuple[str, int]:
    """
    Fetch the table index and table name from the given line
    :param splitted_line: file line in list form
    :param prefix_word: word occurring before table name
    :return: table name and its index position
    """
    table_name_index = splitted_line.index(prefix_word) + 1
    table_name = splitted_line[table_name_index].split('.')[1]
    return table_name, table_name_index


def _replace_value_in_list(data_list: list, index: int, value: str) -> list:
    """
    replaces the value at provided index
    :param data_list: data list
    :param index: index to replace
    :param value: value to insert
    :return: modified list
    """
    data_list.remove(data_list[index])
    data_list.insert(index, value)
    return data_list


def _append_list_item(data_dict: dict, key: str, value: str) -> dict:
    """
    append the data in form of list to the provided dictionary
    :param data_dict: data dict to update
    :param key: key
    :param value: value to append
    :return: modified dict
    """
    if data_dict.get(key):
        data_dict[key].append(value)
    else:
        data_dict.update({key: [value]})
    return data_dict


def _process_ddl_file(ddl_file: typing.IO, target_path: str) -> Tuple[list, defaultdict]:
    """
    Read the ddl file and create sql models of those
    :param ddl_file: postgres ddl dump file
    :param target_path: final destination where to put models
    :return: list of all models which are needed as source
    """
    table_name_list = []
    sql_file_data = defaultdict(dict)
    while True:
        line = ddl_file.readline()
        if not line:
            break
        if 'CREATE TABLE' in line:
            table_name = _get_table_info_from_prefix(splitted_line=line.split(), prefix_word='TABLE')[0]
            table_name_list.append({'name': f'raw_import_{table_name}'})
            select_command_list = ['select\n']
            insert_comma = False
            line = ddl_file.readline()
            while ');' not in line:
                if insert_comma:
                    select_command_list.append(',\n')
                else:
                    insert_comma = True
                select_command_list.append(_get_column(line))
                line = ddl_file.readline()
            select_command_list.append("\nfrom {{source('public', 'raw_import_" + table_name + "')}}\n")
            sql_file_data[table_name].update({'select': select_command_list})
        elif 'ALTER TABLE' in line:
            splitted_line = line.split()
            line = ddl_file.readline()
            if 'PRIMARY KEY' in line:
                table_name, table_name_index = _get_table_info_from_prefix(
                    splitted_line=splitted_line,
                    prefix_word='ONLY'
                )
                splitted_line = _replace_value_in_list(
                    data_list=splitted_line,
                    index=table_name_index,
                    value='{{ this }}'
                )
                splitted_line = splitted_line + line.split()
                line = ' '.join(splitted_line)
                sql_file_data[table_name] = _append_list_item(
                    data_dict=sql_file_data[table_name],
                    key='post_hooks',
                    value=line.split(';')[0]
                )
        elif 'CREATE INDEX' in line or 'CREATE UNIQUE INDEX' in line:
            splitted_line = line.split()
            table_name, table_name_index = _get_table_info_from_prefix(
                splitted_line=splitted_line,
                prefix_word='ON'
            )
            splitted_line = _replace_value_in_list(
                data_list=splitted_line,
                index=table_name_index,
                value='{{ this }}'
            )
            splitted_line.insert(splitted_line.index('INDEX') + 1, 'IF NOT EXISTS')
            line = ' '.join(splitted_line)
            sql_file_data[table_name] = _append_list_item(
                data_dict=sql_file_data[table_name],
                key='post_hooks',
                value=line.split(';')[0]
            )
    return table_name_list, sql_file_data


def _populate_sources_file(sources_dir, table_name_list):
    """
    Populate the sources.yml file
    :param sources_dir: path where to store sources.yml file
    :param table_name_list: list of table names which act as dict
    """
    with open(f"{os.path.join(sources_dir, 'sources.yml')}", 'w') as sources_file:
        yaml.safe_dump({'version': 2, 'sources': [{'name': 'public', 'tables': table_name_list}]}, sources_file)


def _create_models_file(sql_file_data: dict, target_path: str) -> None:
    """
    Creates the dbt models file in the target directory with the data provided in sql_file_data
    :param sql_file_data: data dict to write in models file
    :param target_path: destination directory where all the sql files are stored
    """
    for table_name, values in sql_file_data.items():
        with open(f'{os.path.join(target_path, table_name)}.sql', 'w') as out_file:
            if values.get('post_hooks'):
                out_file.write('{{ config(\n\tpost_hook=[')
                insert_comma = False
                for data in values['post_hooks']:
                    if insert_comma:
                        out_file.write(f',')
                    else:
                        insert_comma = True
                    out_file.write(f'\n\tafter_commit(\"{data}\")')
                out_file.write('\n]\n) }}\n')
            if values.get('select'):
                for data in values['select']:
                    out_file.write(data)


def main():
    """
    Script to create modals file from ddl file
    """
    args = _define_arguments()
    file = open(args.ddl_file, 'r')
    table_name_list, sql_file_data = _process_ddl_file(ddl_file=file, target_path=args.target)
    _create_models_file(sql_file_data=sql_file_data, target_path=args.target)
    if args.sources_dir:
        _populate_sources_file(sources_dir=args.sources_dir, table_name_list=table_name_list)


if __name__ == '__main__':
    main()
