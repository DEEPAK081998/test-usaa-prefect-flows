from prefect.run_configs import ECSRun
from prefect.storage import S3


def get_default_run_config(labels: [str] = None, image: str = None):
    """
    Build and returns the ECSRun task
    :param labels: extra labels to provide to ecs run
    :param image: image to use ecs run with
    :return: ECSRun task
    """
    return ECSRun(
        labels=labels,
        image=image
    )


def get_default_storage(bucket: str):
    """
    Build and return the S3 storage
    :param bucket: bucket name
    :return S3 storage task
    """
    return S3(
        bucket=bucket,
    )
