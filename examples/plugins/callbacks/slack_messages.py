import os
import urllib.parse

from airflow.hooks.base_hook import BaseHook
from slack_webhooks.slack_webhook_operator import SlackWebhookOperator

AIRFLOW_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")
AIRFLOW_URL = "http://localhost:8080"


def send_slack_message(context, message, **kwargs):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    connection_id = kwargs["connection_id"]

    context["task_instance"].xcom_push(key=dag_id, value=True)
    timestamp = context["ts"]
    urlencoded_timestamp = urllib.parse.quote(timestamp)
    logs_url = "<{}/log?dag_id={}&task_id={}&execution_date={}>|Logs".format(
        AIRFLOW_URL, dag_id, task_id, urlencoded_timestamp
    )
    slack_alert = SlackWebhookOperator(
        task_id=task_id,
        slack_webhook_conn_id=connection_id,
        message=f"{message.format(dag_id=dag_id, task_id=task_id)} ({logs_url})",
    )
    slack_alert.execute(context=context)


def inform_success(context, message=None, **kwargs):
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
