from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from requests.structures import CaseInsensitiveDict

headers = CaseInsensitiveDict()

with DAG(dag_id='trigger_airbyte_job_example2',
         default_args={'owner': 'airflow'},
         schedule_interval='@hourly',
         start_date=days_ago(1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_db_mirroring_test',
        airbyte_conn_id='airbyte_conn_example2',
        connection_id='ae731fab-7df7-4e35-8567-1beb81ef7dc4',
        asynchronous=False,
        timeout=3600,
        wait_seconds=20
    )