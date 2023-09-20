import pendulum

UPLOAD_START_DATE = '2022-10-01'

# Airflow
start_date = pendulum.datetime(2022, 10, 10)
SCHEDULE = '0 12 * * *'
TEMPLATE_SEARCHPATH = 'plugins/pennymac_clicks/sql'

# SFTP
SFTP_FOLDER = 'inbound/'

# S3
PENNYMAC_CLICKS_PREFIX = 'Homestory_Clicks_'
REGION = 'us-east-1'
RAW_FOLDER = 'pennymac_clicks/raw'
RAW_PENNYMAC_CLICKS_PREFIX = f"{RAW_FOLDER}/{PENNYMAC_CLICKS_PREFIX}"

# Postgres
SCHEMA = 'public'
TABLE_NAME = 'rep_292_pennymac_clicks'
TABLE_EMPTY_KEYS = f"{TABLE_NAME}_empty_keys"

# Data Transformation
COLS_RENAMING = {
    'EMAILADDRESS': 'email',
    'FIRSTNAME': 'first_name',
    'LASTNAME': 'last_name',
}

# AWS
SECRETS_STACK_NAME = 'mdp-secrets-sandbox-stack'
SENDGRID_SECRET_STACK_KEY = 'SendGridAPIKeySecret'
