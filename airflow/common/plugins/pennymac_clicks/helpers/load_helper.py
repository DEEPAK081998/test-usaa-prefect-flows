import logging
from typing import List

import pandas as pd
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pennymac_clicks.config import (RAW_FOLDER, SCHEMA, TABLE_EMPTY_KEYS, TABLE_NAME,
                                    UPLOAD_START_DATE)
from pennymac_clicks.helpers.postgres_helper import get_contacts_to_add
from pennymac_clicks.helpers.sendgrid_helper import SendGridHelper
from pennymac_clicks.helpers.tools import check_file_is_not_empty, extract_date, get_filename
from pennymac_clicks.helpers.variable_helper import (bucket, contact_list_id, postgres_conn_id,
                                                     sftp_conn_id)


@task
def load_sftp_to_s3(sftp_s3_diff: List[str]) -> bool:
    """Load files from SFTP to S3."""
    # Importing it at the top was causing import error on the Airflow UI. Importing it locally for now
    # to resolve the issue on the UI. Further details are mentioned in
    # (https://homestory.atlassian.net/browse/REP-580?focusedCommentId=262730)
    from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator

    files_to_load = sftp_s3_diff
    logging.info(f"Number of files to load to S3: {len(files_to_load)}")
    for filename in files_to_load:
        sftp_path = f"inbound/{filename}"
        s3_key = f"{RAW_FOLDER}/{filename}"
        logging.info(f"Copying from sftp to s3. SFTP: {sftp_path} --> S3: {s3_key}")
        copy_operator = SFTPToS3Operator(
            task_id=f"copy_{filename}",
            sftp_conn_id=sftp_conn_id(),
            sftp_path=sftp_path,
            s3_conn_id='aws_default',
            s3_bucket=bucket(),
            s3_key=s3_key,
        )
        copy_operator.execute(dict())

    return True


@task
def load_s3_to_postgres(s3_postgres_diff: List[str]) -> bool:
    """Read dataframe from S3 and write it to postgres."""

    def download_from_s3(file_key) -> str:
        """Download file from S3."""

        s3hook = S3Hook()
        temp_file_path = s3hook.download_file(
            key=file_key,
            bucket_name=bucket()
        )
        return temp_file_path

    def read_and_transform_df(file_key):
        """Read dataframe from S3 and add columns."""

        df = pd.read_csv(temp_file_path)

        # Lowercase columns
        df.columns = [col.lower() for col in df.columns]

        # Add columns
        filename = get_filename(file_key)
        df['_filename'] = filename
        df['_loaded_by_pennymac'] = extract_date(filename)
        return df

    def write_df_to_postgres(df: pd.DataFrame, table_name: str):
        """Write dataframe to postgres."""

        hook = PostgresHook(postgres_conn_id=postgres_conn_id())
        engine = hook.get_sqlalchemy_engine()
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists='append'
        )

    def update_empty_keys_in_postgres(new_empty_keys: List[str]):
        logging.info(f"new_empty_keys = {new_empty_keys}")
        df_empty_keys = pd.DataFrame(new_empty_keys, columns=['empty_keys'])
        write_df_to_postgres(df_empty_keys, TABLE_EMPTY_KEYS)

    def select_files_after_start_date(file: List[str]) -> List[str]:
        """Select files after specified start date."""

        selected_files = list()
        for file in file:
            if extract_date(file) >= UPLOAD_START_DATE:
                selected_files.append(file)

        return selected_files

    if s3_postgres_diff:
        latest_files = select_files_after_start_date(s3_postgres_diff)
        file_keys = [f"{RAW_FOLDER}/{file}" for file in latest_files]

        new_empty_keys = list()
        for file_key in file_keys:
            logging.info(f"Loading from S3 to Postgres. File key: {file_key}")
            temp_file_path = download_from_s3(file_key)
            if check_file_is_not_empty(temp_file_path):
                df = read_and_transform_df(file_key)
                write_df_to_postgres(df, TABLE_NAME)
            else:
                new_empty_keys.append(file_key)
                logging.info(f"Skipped empty file: {file_key}")

        update_empty_keys_in_postgres(new_empty_keys)
    else:
        logging.info('No file to load to Postgres.')

    return True


@task
def add_contacts_to_sendgrid(ok: bool):
    """Add contacts to a list in SendGrid."""

    contacts, emails_to_add = get_contacts_to_add()
    if contacts:
        sg = SendGridHelper()
        sg.add_or_update_a_contact(list_id=contact_list_id(), contacts=contacts)
    else:
        logging.info('No contacts to update. Skipping task.')

    return emails_to_add
