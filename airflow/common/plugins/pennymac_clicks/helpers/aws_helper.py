import json
import logging

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from pennymac_clicks.config import REGION


def read_secret(secret_id: str) -> str:
    """Read secret from AWS Secrets Manager."""

    hook = AwsBaseHook(client_type='secretsmanager')
    client = hook.get_client_type('secretsmanager', region_name=REGION)
    response = client.get_secret_value(SecretId=secret_id)
    secret = response['SecretString']

    if isinstance(secret, str) and '"' in secret:
        logging.info('Secret is string. Using json.loads() to convert to dict.')
        return json.loads(secret)

    return secret
