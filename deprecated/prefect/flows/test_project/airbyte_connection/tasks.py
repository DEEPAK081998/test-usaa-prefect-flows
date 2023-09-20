from datetime import timedelta

from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask


def sync_airbyte_connection():
    return AirbyteConnectionTask(
        max_retries=3,
        retry_delay=timedelta(seconds=20),
    )
