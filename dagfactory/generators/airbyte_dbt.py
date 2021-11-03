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

    def generate_tasks(self, dtks={}) -> Dict:

        """
        Connects to airbyte server, gets sources and creates a new task for each source
        """
        op1 = "airflow.operators.bash_operator.BashOperator"
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            operator_obj: Callable[..., BaseOperator] = import_string(op1)
        except Exception as err:
            raise Exception(f"Failed to import operator: {op1}") from err

        list_names = list(dtks.keys())
        list_tasks = list(dtks.values())
        listin = list(zip(list_names, list_tasks))

        tsk_dict = {}

        vars = [None] * len(listin)
        # New vars for generate
        for x in range(0, len(listin)):
            vars[x] = globals()[f"task_0{x}"] = x

        for x in range(0, len(listin)):
            vars[x]: BaseOperator = DagBuilder.make_task(op1, list_tasks[x])

        for x in range(0, len(listin)):
            tsk_dict[list_names[x]] = vars[x]

        return tsk_dict
