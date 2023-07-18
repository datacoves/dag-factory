import os
import urllib.parse

from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

AIRFLOW_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")


def send_slack_message(context, message, **kwargs):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    connection_id = kwargs["connection_id"]
    slack_webhook_token = BaseHook.get_connection(connection_id).password
    channel = BaseHook.get_connection(connection_id).login

    context["task_instance"].xcom_push(key=dag_id, value=True)
    timestamp = context["ts"]
    urlencoded_timestamp = urllib.parse.quote(timestamp)
    logs_url = "<{}/log?dag_id={}&task_id={}&execution_date={}>".format(
        AIRFLOW_URL, dag_id, task_id, urlencoded_timestamp
    )
    slack_alert = SlackWebhookOperator(
        task_id=task_id,
        webhook_token=slack_webhook_token,
        message=f"{message.format(dag_id=dag_id, task_id=task_id)}. Logs: ({logs_url})",
        channel=channel,
        http_conn_id=connection_id,
    )
    slack_alert.execute(context=context)


def inform_success(context, message=None, **kwargs):
    if context["dag_run"].state == "success":
        send_slack_message(
            context,
            message or "`{dag_id}` has succeded on task: `{task_id}`",
            **kwargs,
        )


def inform_failure(context, message=None, **kwargs):
    send_slack_message(
        context,
        message or "`{dag_id}` has failed on task: `{task_id}`",
        **kwargs,
    )


def inform_retry(context, message=None, **kwargs):
    send_slack_message(**kwargs)


def inform_sla_miss(context, message=None, **kwargs):
    send_slack_message(
        context,
        message or "`{dag_id}` (task `{task_id}`) has missed it's SLA",
        **kwargs,
    )
