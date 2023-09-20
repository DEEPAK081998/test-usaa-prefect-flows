import json
import os
from typing import Tuple
from urllib.parse import urlparse

import boto3


def put_data_dict_in_file(file_path: str, data_dict: dict) -> None:
    """
    Dumps the data dict in the given file
    :param file_path: file path where to write data
    :param data_dict: data to dump in file
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as fp:
        json.dump(data_dict, fp, indent=2)


def extract_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Extract bucket name and s3 path from s3 uri
    :param s3_uri: s3 uri
    :return: bucket name and path
    """
    s3_parsed_data = urlparse(s3_uri, allow_fragments=False)
    return (
        s3_parsed_data.netloc,
        s3_parsed_data.path[1:] if s3_parsed_data.path.startswith('/') else s3_parsed_data.path
    )


def upload_file_to_s3(file_name: str, bucket: str, key: str) -> None:
    """
    Upload given file to s3 bucket
    :param file_name: local file path
    :param bucket: s3 bucket name
    :param key: s3 path
    """
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file_name, bucket, key)


def download_file_from_s3(file_name: str, bucket: str, key: str) -> None:
    """
    Download file from s3 bucket
    :param file_name: local file path
    :param bucket: s3 bucket name
    :param key: s3 path
    """
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, key, file_name)
