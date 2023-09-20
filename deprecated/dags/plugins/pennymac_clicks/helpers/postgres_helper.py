import logging
from typing import Dict, List

import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pennymac_clicks.config import TABLE_EMPTY_KEYS, TABLE_NAME, UPLOAD_START_DATE
from pennymac_clicks.helpers.tools import convert_list_of_tuples_to_list, create_values_placeholder
from pennymac_clicks.helpers.variable_helper import postgres_conn_id


def _execute_query(query: str, vars):
    """Execute a query by using Psycopg execute() method with a Postgres hook."""

    hook = PostgresHook(postgres_conn_id=postgres_conn_id())
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query=query, vars=vars)
    connection.commit()
    cursor.close()
    connection.close()


@task
def list_files_in_postgres() -> List[str]:
    """Get filenames in postgres."""

    hook = PostgresHook(postgres_conn_id=postgres_conn_id())
    result = hook.get_records(f"SELECT distinct(_filename) FROM {TABLE_NAME}")
    files_in_postgres = convert_list_of_tuples_to_list(result)
    logging.info(f"files_in_postgres = {files_in_postgres}")
    return files_in_postgres


@task
def list_empty_keys() -> List[str]:
    """List empty keys from postgres."""

    hook = PostgresHook(postgres_conn_id=postgres_conn_id())
    result = hook.get_records(f"SELECT distinct(empty_keys) FROM {TABLE_EMPTY_KEYS}")
    empty_keys = convert_list_of_tuples_to_list(result)
    logging.info(f"empty_keys = {empty_keys}")

    return empty_keys


def get_contacts_to_add():
    """Get contacts to add to SendGrid.

    A contact is composed of: email, first_name, last_name."""

    def check_contacts_format(contacts: List[Dict]):
        """Check that contacts have the three required fields."""
        first_contact = contacts[0]
        assert set(['email', 'first_name', 'last_name']) == set(first_contact.keys())

    query = f"""
        SELECT emailaddress as email, firstname as first_name, lastname as last_name
        FROM {TABLE_NAME}
        WHERE _loaded_by_pennymac >= '{UPLOAD_START_DATE}'
            AND "_loaded_to_sendgrid" IS NULL
    """
    logging.info(f"query = {query}")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id())
    df_contacts: pd.DataFrame
    df_contacts = hook.get_pandas_df(sql=query)
    if not df_contacts.empty:
        df_contacts.drop_duplicates(subset='email', inplace=True)
        emails_to_add = tuple(df_contacts['email'].tolist())

        logging.info(f"Number of contacts to add: {df_contacts.shape[0]}")
        logging.info(f"Emails to add: {emails_to_add}")
        contacts = df_contacts.to_dict(orient='records')
        check_contacts_format(contacts)
        logging.info(f"contacts = {contacts}")
    else:
        contacts = None
        emails_to_add = None
    return contacts, emails_to_add


@task
def register_added_contacts(emails_to_add: List[str]):
    """Update database with contacts that were added. Allows management of the added contacts.

    https://www.postgresql.org/docs/current/sql-update.html
    """
    added_emails = emails_to_add
    if added_emails:
        logging.info(f"Number of emails to register as added: {len(added_emails)}")
        logging.info(f"added_emails = {added_emails}")
        UPDATE_STATEMENT = f"""
            UPDATE {TABLE_NAME}
            SET "_loaded_to_sendgrid" = CURRENT_TIMESTAMP
            WHERE "emailaddress" IN {create_values_placeholder(added_emails)}
            """
        logging.info(f"Update statement: {UPDATE_STATEMENT}")

        _execute_query(query=UPDATE_STATEMENT, vars=added_emails)
    else:
        logging.info('No email to register as added contact.')
