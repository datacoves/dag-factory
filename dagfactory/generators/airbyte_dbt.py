from typing import Dict
import os
import importlib
from dagfactory.dagbuilder import DagBuilder


"""Module contains code for generating tasks and constructing a DAG"""
from datetime import timedelta, timezone, datetime
from typing import Any, Callable, Dict, List, Union

import os
import importlib

from airflow import DAG, configuration
from airflow.models import Variable
from airflow import DAG, configuration
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# from airflow.sensors.http_sensor import HttpSensor
# from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.module_loading import import_string
from airflow import __version__ as AIRFLOW_VERSION
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string
from airflow import __version__ as AIRFLOW_VERSION


class AirbyteDbtGenerator:
    def __init__(self, dag_builder):
        self.dag_builder = dag_builder

    def generate_tasks(self, dag_ob) -> Dict:

        """
        Connects to airbyte server, gets sources and creates a new task for each source
        """
        op1 = "airflow.operators.bash_operator.BashOperator"
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            operator_obj: Callable[..., BaseOperator] = import_string(op1)
        except Exception as err:
            raise Exception(f"Failed to import operator: {op1}") from err

        task_1_params = {
            "bash_command": "echo 1",
            "task_id": "task_1",
            "dag": dag_ob,
        }

        task_2_params = {
            "bash_command": "echo 2",
            "task_id": "task_2",
            "dag": dag_ob,
        }

        task_3_params = {
            "bash_command": "echo 3",
            "task_id": "task_3",
            "dag": dag_ob,
        }

        task_1: BaseOperator = DagBuilder.make_task(op1, task_1_params)
        task_2: BaseOperator = DagBuilder.make_task(op1, task_2_params)
        task_3: BaseOperator = DagBuilder.make_task(op1, task_3_params)

        tsk_dict = {}

        tsk_dict[task_1.task_id] = task_1
        tsk_dict[task_2.task_id] = task_2
        tsk_dict[task_3.task_id] = task_3

        return tsk_dict
