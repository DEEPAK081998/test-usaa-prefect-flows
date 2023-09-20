import time
import logging
from typing import Any
from types import FunctionType
from airflow import AirflowException
from botocore.exceptions import ClientError

def retry_mechanism(func: FunctionType, max_retry_count: int = 10) -> Any:
    """
    Decorator to integrate retry mechanism to the passed function
    :param func: Function to be integrated with retry mechanism
    :param max_retry_count: Max retry count
    """
    def inner(*args, **kwargs):
        logging.info(f'Processing {func.__name__} with args {args} and kwargs {kwargs}')
        retry_count = 0
        while retry_count <= max_retry_count:
            try:
                return func(*args, **kwargs)
            except KeyError as exception:
                raise AirflowException(f"Error occurred while calling {func.__name__}: {exception}")
            except ClientError as exception:
                if exception.response['Error']['Code'] == 'ThrottlingException':
                    sleep_duration = 30 + (2 ** retry_count)
                    retry_count += 1
                    logging.info(f'Retrying function {func.__name__} after {sleep_duration} seconds.')
                    time.sleep(sleep_duration)
                else:
                    raise exception
        raise Exception(f'Max retries exceeded for function: {func.__name__}')
    return inner
