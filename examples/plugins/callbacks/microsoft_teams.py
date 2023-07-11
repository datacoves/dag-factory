import os
import urllib.parse

from ms_teams.ms_teams_webhook_operator import MSTeamsWebhookOperator

AIRFLOW_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")


def send_teams_message(context, dag_id, task_id, message, theme_color, connection_id):
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


def _prepare_and_send_message(
    context, keyword_args, replacement_message, replacement_color
):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    connection_id = keyword_args["connection_id"]
    message = keyword_args.get(
        "message", replacement_message.format(dag_id=dag_id, task_id=task_id)
    )
    theme_color = keyword_args.get("color", replacement_color)
    send_teams_message(context, dag_id, task_id, message, theme_color, connection_id)


def inform_success(context, **kwargs):
    replacement_message = "`{dag_id}` has succeded on task: `{task_id}`"
    replacement_color = "00FF00"
    _prepare_and_send_message(context, kwargs, replacement_message, replacement_color)


def inform_failure(context, **kwargs):
    replacement_message = "`{dag_id}` has failed on task: `{task_id}`"
    replacement_color = "FF0000"
    _prepare_and_send_message(context, kwargs, replacement_message, replacement_color)


def inform_retry(context, **kwargs):
    replacement_message = "`{dag_id}` is retrying task: `{task_id}`"
    replacement_color = "FFEA00"
    _prepare_and_send_message(context, kwargs, replacement_message, replacement_color)


def inform_sla_miss(context, **kwargs):
    replacement_message = "`{dag_id}` (task `{task_id}`) has missed it's SLA"
    replacement_color = "FFA200"
    _prepare_and_send_message(context, kwargs, replacement_message, replacement_color)
