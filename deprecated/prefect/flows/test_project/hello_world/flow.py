import prefect
from prefect import Flow
from prefect import task


@task
def hello_world():
    """
    A sample function to log info message
    """
    logger = prefect.context.get("logger")
    logger.info(f'HELLO WORLD, prefect version:- {prefect.__version__}')


flow_name = 'hello-world'
with Flow(flow_name) as flow:
    hello_world()
