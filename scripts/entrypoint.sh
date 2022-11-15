#!/usr/bin/env bash

export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init
# for airflow 2.0
airflow users create --username admin \
    --firstname test \
    --lastname test \
    --role Admin \
    --email admin@example.org \
    -p admin

airflow connections add --conn-host airbyte-server --conn-port 8001 --conn-type airbyte airbyte_conn_example

airflow scheduler &
exec airflow webserver
