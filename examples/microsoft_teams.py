import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from ms_teams_webhook_operator import MSTeamsWebhookOperator

AIRFLOW_URL = os.environ.get("AIRFLOW_URL")


def send_message(context):
    dag_id = context["dag_run"].dag_id

    task_id = context["task_instance"].task_id
    context["task_instance"].xcom_push(key=dag_id, value=True)

    logs_url = f"https://{AIRFLOW_URL}/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={context['ts']}"
    ms_teams_notification = MSTeamsWebhookOperator(
        task_id="msteams_notify_failure",
        trigger_rule="all_done",
        message="`{}` has failed on task: `{}`".format(dag_id, task_id),
        button_text="View log",
        button_url=logs_url,
        theme_color="FF0000",
        http_conn_id="msteams_saas_notifications",
    )
    ms_teams_notification.execute(context)
