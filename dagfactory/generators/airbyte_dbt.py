from datetime import timedelta, datetime
from typing import Any, Callable, Dict, List, Union
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string
from airflow import __version__ as AIRFLOW_VERSION
from ..dagfactory import DagBuilder

# IMPORTS FOR TESTING
import ipdb
#from airflow.operators.bash_operator import BashOperator


class AirbyteDbtGenerator:
    def __init__(self, dag_builder):
        self.dag_builder = dag_builder

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        #ipdb.set_trace()
        print("generating tasks")

        # Pop AirbyteDbtGenerator as it doesn't work: Dag-Factory always expects an Operator
        params.pop('generator')
        
        
        tasks: Dict[str, BaseOperator] = {}
        for i in range(1, 3):
            params["bash_command"] = f"echo {i}"
            params["task_id"] = f"task_{i}"
            task: BaseOperator = DagBuilder.make_task(
                    operator="airflow.operators.bash_operator.BashOperator", task_params=params
            )
            tasks[params["task_id"]] = task

        return tasks
        """
        Connects to airbyte server, gets sources and creates a new task for each source
        """
        # call self.dag_builder to generate new tasks
        #pass
