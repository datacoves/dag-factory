import os
import urllib.parse

from ms_teams_webhook_operator import MSTeamsWebhookOperator

AIRFLOW_URL = os.environ.get("AIRFLOW_URL")


def send_teams_message(context, dag_id, task_id, message, theme_color, connection_id):
    AIRFLOW_URL = os.environ.get("AIRFLOW_URL")
    context["task_instance"].xcom_push(key=dag_id, value=True)
    timestamp = context["ts"]
    urlencoded_timestamp = urllib.parse.quote(timestamp)

    logs_url = "{}/log?dag_id={}&task_id={}&execution_date={}".format(
        AIRFLOW_URL, dag_id, task_id, urlencoded_timestamp
    )
    ms_teams_notification = MSTeamsWebhookOperator(
        task_id=task_id,
        trigger_rule="all_done",
        message=message,
        button_text="View log",
        button_url=logs_url,
        theme_color=theme_color,
        http_conn_id=connection_id,
    )
    ms_teams_notification.execute(context)


def inform_success(context, **kwargs):
    connection_id = kwargs.get("connection_id")
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    message = f"`{dag_id}` has succeded on task: `{task_id}`"
    theme_color = "00FF00"
    send_teams_message(context, dag_id, task_id, message, theme_color, connection_id)


def inform_failure(context, **kwargs):
    connection_id = kwargs.get("connection_id")
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    message = f"`{dag_id}` has failed on task: `{task_id}`"
    theme_color = "FF0000"
    send_teams_message(context, dag_id, task_id, message, theme_color, connection_id)
