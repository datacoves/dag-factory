import datetime

import pytz
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = "slack_connection"


def convert_datetime(datetime_string):
    return datetime_string.astimezone(pytz.timezone("America/Denver")).strftime(
        "%b-%d %H:%M:%S"
    )


##### Slack Alerts #####
def slack_fail_alert(context):
    """Adapted from https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
    Sends message to a slack channel.
       If you want to send it to a "user" -> use "@user",
           if "public channel" -> use "#channel",
           if "private channel" -> use "channel"
    """

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id="slack_fail",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username="Airflow Notifications Dev",
        http_conn_id=SLACK_CONN_ID,
    )

    return slack_alert.execute(context=context)


default_args = {"on_failure_callback": slack_fail_alert}

with DAG(
    dag_id="python_sample_slack_dag",
    default_args=default_args,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["version_17"],
    description="Sample python dag dbt run",
    schedule_interval="0 0 1 */12 *",
) as dag:
    failing_task = BashOperator(task_id="failing_task", bash_command="echoSuccess")

    # failing_task = BashOperator(
    #     task_id = "failing_task",
    #     bash_command = "some_non_existant_command"
    # )

    # Call the helper function to set the callbacks for all tasks
    # set_task_callbacks(dag)  # If we want to set per-task callback

    # runs failing task
    # successful_task >> failing_task

    failing_task
