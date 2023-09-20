from airflow.models import Variable


def main_variable():
    """Get main_variable from Airflow Variables."""
    return Variable.get('REP-292', deserialize_json=True)


def postgres_secret_id():
    """Get postgres_secret_id from Airflow Variables."""
    return main_variable()['postgres_secret_id']


def postgres_conn_id():
    """Get postgres_conn_id from Airflow Variables."""
    return main_variable()['postgres_conn_id']


def bucket():
    """Get bucket from Airflow Variables."""
    return main_variable()['bucket']


def contact_list_id():
    """Get contact_list_id from Airflow Variables."""
    return main_variable()['contact_list_id']


def sendgrid_secret_id():
    """Get sendgrid_secret_id from Airflow Variables."""
    return main_variable()['sendgrid_secret_id']


def sftp_secret_id():
    """Get sftp_secret_id from Airflow Variables."""
    return main_variable()['sftp_secret_id']


def sftp_conn_id():
    """Get sftp_conn_id from Airflow Variables."""
    return main_variable()['sftp_conn_id']
