import boto3
import pandas as pd
import logging
from botocore.exceptions import ClientError


def read_file_from_s3(s3_bucket: str, s3_key: str, binary: bool = False):
    """Read file from S3."""

    s3_client = boto3.client('s3')
    logging.info(f"bucket, s3_key = {s3_bucket, s3_key}")
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        if binary:
            return response.get("Body").read()
        else:
            return response.get("Body").read().decode('utf-8')
    else:
        print(f"Failed to read file from S3. Status: {status}")


def read_csv_from_s3(s3_bucket: str, s3_key: str, local_run: bool = False):
    """Read csv from S3."""

    try:
        s3_client = boto3.client('s3')
        logging.info(f"bucket, s3_key = {s3_bucket, s3_key}")
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            df = pd.read_csv(response.get("Body"))
            return df
        else:
            print(f"Failed to read csv from S3. Status: {status}")
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            msg = f"Did not find object ({s3_bucket}/{s3_key}). Returning empty"
            logging.warning(msg)
            if not local_run:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException
            return pd.DataFrame()
        else:
            raise
