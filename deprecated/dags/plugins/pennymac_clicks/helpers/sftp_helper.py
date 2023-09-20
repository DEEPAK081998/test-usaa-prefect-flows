import logging
from typing import List

from airflow.decorators import task
from pennymac_clicks.config import PENNYMAC_CLICKS_PREFIX, SFTP_FOLDER
from pennymac_clicks.helpers.tools import select_csv, select_with_prefix
from pennymac_clicks.helpers.variable_helper import sftp_conn_id


@task
def list_files_in_sftp() -> List[str]:
    """List files available in SFTP."""
    # Importing it at the top was causing import error on the Airflow UI. Importing it locally for now
    # to resolve the issue on the UI. Further details are mentioned in
    # (https://homestory.atlassian.net/browse/REP-580?focusedCommentId=262730)
    from airflow.providers.sftp.hooks.sftp import SFTPHook

    hook = SFTPHook(sftp_conn_id())
    files_in_sftp = hook.list_directory(path=SFTP_FOLDER)
    files_in_sftp = select_csv(files_in_sftp)
    csv_files_in_sftp = select_with_prefix(files_in_sftp, PENNYMAC_CLICKS_PREFIX)

    logging.info(f"Number of all .csv files in SFTP: {len(files_in_sftp)}")
    logging.info(f"Number of Homestory_Clicks_***.csv files in SFTP: {len(csv_files_in_sftp)}")

    return csv_files_in_sftp
