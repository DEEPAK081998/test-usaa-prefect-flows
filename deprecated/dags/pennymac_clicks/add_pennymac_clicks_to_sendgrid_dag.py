import logging

from airflow import DAG

from pennymac_clicks.config import start_date
from pennymac_clicks.helpers.comparison_helper import compare_s3_and_postgres, compare_sftp_and_s3
from pennymac_clicks.helpers.load_helper import (add_contacts_to_sendgrid, load_s3_to_postgres,
                                                 load_sftp_to_s3)
from pennymac_clicks.helpers.postgres_helper import (list_empty_keys, list_files_in_postgres,
                                                     register_added_contacts)
from pennymac_clicks.helpers.s3_helper import list_files_in_s3
from pennymac_clicks.helpers.sendgrid_helper import confirm_added_contacts
from pennymac_clicks.helpers.sftp_helper import list_files_in_sftp

logging.getLogger('boto').setLevel(logging.WARNING)


with DAG(
    dag_id='REP_292-add_pennymac_clicks_to_sendgrid',
    description='Add PennyMac contacts (clicks) to SendGrid from SFTP.',
    start_date=start_date,
    tags=['pennymac', 'sftp', 'sendgrid']
) as dag:
    files_in_sftp = list_files_in_sftp()
    files_in_s3 = list_files_in_s3()
    sftp_s3_diff = compare_sftp_and_s3(files_in_sftp, files_in_s3)

    ok_loading_to_s3 = load_sftp_to_s3(sftp_s3_diff)
    files_in_s3__1 = list_files_in_s3(ok_loading_to_s3)

    files_in_postgres = list_files_in_postgres()
    empty_files_in_s3 = list_empty_keys()
    s3_postgres_diff = compare_s3_and_postgres(
        files_in_postgres,
        files_in_s3__1,
        empty_files_in_s3)

    ok_loading_to_postgres = load_s3_to_postgres(s3_postgres_diff)

    emails_to_add = add_contacts_to_sendgrid(ok_loading_to_postgres)
    register_added_contacts(emails_to_add)
    confirm_added_contacts(emails_to_add)
