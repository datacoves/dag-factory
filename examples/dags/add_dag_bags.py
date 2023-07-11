"""add multiple DAG folders"""
import os
from pathlib import Path

from airflow.models import DagBag

dags_dirs = ["/app/dags_backup/main", "/app/dags_backup/orchestrate"]

for dir in dags_dirs:
    dag_bag = DagBag(Path(dir).expanduser())

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag
