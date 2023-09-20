import logging
from os import path
from typing import List, Tuple

import pandas as pd
from pennymac_clicks.config import COLS_RENAMING


def select_csv(files: List[str]) -> List[str]:
    """Select files with .csv extension."""

    selected_files = [name for name in files if name.endswith('.csv')]
    logging.info(f"Number of selected csv files: {len(selected_files)}")
    return selected_files


def select_with_prefix(files: List[str], prefix: str) -> List[str]:
    """Select files with specified prefix."""

    selected_files = [name for name in files if name.startswith(prefix)]
    logging.info(f"Number of selected csv files: {len(selected_files)}")
    return selected_files


def get_filename(file):
    """Extract filename from fullpath."""
    filename = path.basename(file)
    return filename


def get_filenames(files):
    """Extract filename from fullpath."""
    filenames = [path.basename(file) for file in files]
    return filenames


def transform(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns. Drop duplicates"""

    # Select keys columns from COLS_RENAMING
    df: pd.DataFrame
    df = df_raw[COLS_RENAMING.keys()]

    # Rename columns to SendGrid's format
    df.rename(columns=COLS_RENAMING, inplace=True)

    # Drop row when email is duplicated
    df.drop_duplicates(subset='email', keep='first', inplace=True)

    return df


def convert_list_of_tuples_to_list(list_of_tuples):
    """Convert list of tuples to list."""
    simple_list = [item[0] for item in list_of_tuples]
    return simple_list


def check_file_is_not_empty(filepath: str) -> bool:
    with open(filepath, 'r') as f:
        content = f.read()
        has_comma = ',' in content
        file_is_not_empty = has_comma
        return file_is_not_empty


def extract_date(filename: str) -> str:
    """Extract date from filename."""

    left_length = len('Homestory_Clicks_')
    date_str = filename[left_length:left_length+8]

    return date_str


def create_values_placeholder(values: Tuple):
    """Create values placeholder."""

    values_placeholder = f"({', '.join(['%s']*len(values))})"
    return values_placeholder
