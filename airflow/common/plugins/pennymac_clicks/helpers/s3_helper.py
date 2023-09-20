import logging
from typing import List

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pennymac_clicks.config import RAW_PENNYMAC_CLICKS_PREFIX
from pennymac_clicks.helpers.tools import get_filenames, select_csv
from pennymac_clicks.helpers.variable_helper import bucket


@task
def list_files_in_s3(ok: bool = False) -> List[str]:
    """List files in bucket having RAW_PENNYMAC_CLICKS_PREFIX prefix."""

    logging.info(f"Listing files in {bucket()}{RAW_PENNYMAC_CLICKS_PREFIX}")
    s3_hook = S3Hook()
    keys = s3_hook.list_keys(
        bucket_name=bucket(),
        prefix=RAW_PENNYMAC_CLICKS_PREFIX
    )
    files_in_s3 = select_csv(keys)
    csvs_filenames = get_filenames(files_in_s3)
    logging.info(f"CSVs in S3: {csvs_filenames}")

    return csvs_filenames
