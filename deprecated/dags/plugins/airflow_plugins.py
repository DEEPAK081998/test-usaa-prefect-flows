import json
import logging
from typing import Any, Dict

from airflow.models import Variable
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

from constants import CUSTOM_AIRFLOW_CONSTANTS, SNS_TOPIC_ARN_VARIABLE_NAME_ON_AIRFLOW

logger = logging.getLogger('airflow.task')

def get_airflow_dag_default_args() -> Dict[str,Any]:
    """
    Returns default args for airflow dag
    :return: Airflow default args dictionary
    """
    return {
        'on_success_callback': None,
        'on_failure_callback': _sns_task_failure_alert,
    }

def _get_variable_value_from_airflow(key: str, default=None) -> Any:
    """
    Get variable value from airflow
    :param key: Key of the variable on airflow
    :param default: Default value to be set if variable is not present
    :return: Variable value on airflow
    """
    return Variable.get(key, default_var=default)

def _sns_task_failure_alert(context: Dict) -> Any:
    """
    Triggers SNS notification for a failed airflow task
    :param context: Airflow task context object
    :return: SNSPublishOperator Message
    """
    sns_variable_name = CUSTOM_AIRFLOW_CONSTANTS.get(SNS_TOPIC_ARN_VARIABLE_NAME_ON_AIRFLOW)
    sns_target_arn = _get_variable_value_from_airflow(sns_variable_name)
    if sns_target_arn:
        sns_msg = {
            "reason": 'airflow_task_failure',
            "task": context.get('task_instance').task_id,
            "dag": context.get('task_instance').dag_id,
            "exec_date": str(context.get('execution_date')),
            "exception": str(context.get('exception')),
            "log_url": context.get('task_instance').log_url,
        }
        failed_alert = SnsPublishOperator(
            task_id='sns_task_failure_alert',
            target_arn=sns_target_arn,
            message=json.dumps(sns_msg)
        )
        return failed_alert.execute(context=context)
    else:
        logger.warn('SNS topic arn is absent in variables, could not send failure alert.')
