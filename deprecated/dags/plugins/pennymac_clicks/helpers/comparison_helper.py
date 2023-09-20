import logging
from typing import List

from airflow.decorators import task

from pennymac_clicks.helpers.tools import get_filenames


@task
def compare_sftp_and_s3(files_in_sftp: List[str], files_in_s3: List[str]) -> List[str]:
    """Get list of files that do not exist in S3, but exist in SFTP."""

    sftp_s3_diff = list(set(files_in_sftp).difference(set(files_in_s3)))
    num_files = {
        'sftp': len(files_in_sftp),
        's3': len(files_in_s3),
        'sftp-s3': len(sftp_s3_diff),
    }

    logging.info(f"Number of files: {num_files}")
    return sftp_s3_diff


@task
def compare_s3_and_postgres(
        files_in_postgres: List[str],
        files_in_s3: List[str],
        empty_files_in_s3: List[str]) -> List[str]:
    """Get list of files that do not exist in Postgres, but exist in S3."""

    files_empty = get_filenames(empty_files_in_s3)
    s3_postgres_diff = list(
        set(files_in_s3)
        .difference(set(files_in_postgres))
        .difference(set(files_empty))
    )

    logging.info(f"empty_files_in_s3 = {empty_files_in_s3}")
    logging.info(f"files_in_s3 = {files_in_s3}")
    logging.info(f"files_in_postgres = {files_in_postgres}")
    logging.info(f"s3_postgres_diff = {s3_postgres_diff}")
    num_files = {
        's3': len(files_in_s3),
        'postgres': len(files_in_postgres),
        'empty_files_in_s3': len(empty_files_in_s3),
        's3-postgres-empty_files_in_s3': len(s3_postgres_diff),
    }

    logging.info(f"Number of files: {num_files}")
    return s3_postgres_diff
