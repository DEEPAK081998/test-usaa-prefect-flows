import datetime
from typing import List

from airflow.decorators import task

from airflow import DAG


COPY_PARAMS = {
    's3_bucket': 'mdp-utils-sandbox-partner-email-bucket',
    's3_key': 'test/csv_for_test.csv',
    'sftp_path': f"test/test_{datetime.datetime.today()}.csv",
    'sftp_conn_id': 'sftp_hs_airflow',  # this is a testing SFTP partition `airflow`
}


@task
def list_files_in_sftp() -> List[str]:
    """List files available in SFTP."""
    # Importing it at the top was causing import error on the Airflow UI. Importing it locally for now
    # to resolve the issue on the UI. Further details are mentioned in
    # (https://homestory.atlassian.net/browse/REP-580?focusedCommentId=262730)
    from airflow.providers.sftp.hooks.sftp import SFTPHook

    hook = SFTPHook(COPY_PARAMS['sftp_conn_id'])
    files_in_sftp = hook.list_directory(path='test/')

    return files_in_sftp


with DAG(
        dag_id="gui_test_sftp_connection",
        start_date=datetime.datetime(2021, 1, 1),
        schedule_interval=None,
        tags=['gui', 'sftp']):

    print(f"COPY_PARAMS = {COPY_PARAMS}")
    from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
    s3_to_sftp_task = S3ToSFTPOperator(
        task_id='copy_from_s3_to_sftp',
        s3_bucket=COPY_PARAMS['s3_bucket'],
        s3_key=COPY_PARAMS['s3_key'],
        sftp_path=COPY_PARAMS['sftp_path'],
        sftp_conn_id=COPY_PARAMS['sftp_conn_id'],
    )
    s3_to_sftp_task >> list_files_in_sftp()
