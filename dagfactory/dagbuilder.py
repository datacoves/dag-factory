"""Module contains code for generating tasks and constructing a DAG"""
import importlib
import os
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Callable, Dict, List, Union

from airflow import DAG
from airflow import __version__ as AIRFLOW_VERSION
from airflow import configuration
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string

# kubernetes operator
try:
    from airflow.kubernetes.pod import Port
    from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
    from airflow.kubernetes.secret import Secret
    from airflow.kubernetes.volume import Volume
    from airflow.kubernetes.volume_mount import VolumeMount
except ImportError:
    from airflow.contrib.kubernetes.secret import Secret
    from airflow.contrib.kubernetes.pod import Port
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv

from kubernetes.client.models import V1Container, V1Pod, V1PodSpec
from packaging import version

from dagfactory import utils

# pylint: disable=ungrouped-imports,invalid-name
# Disabling pylint's ungrouped-imports warning because this is a
# conditional import and cannot be done within the import group above
# TaskGroup is introduced in Airflow 2.0.0
if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
    from airflow.utils.task_group import TaskGroup
else:
    TaskGroup = None
# pylint: disable=ungrouped-imports,invalid-name

# these are params only used in the DAG factory, not in the tasks
SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name"]


class BaseGenerator:
    def generate_tasks(
        params: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        raise NotImplementedError()


class DagBuilder:
    """
    Generates tasks and a DAG from a config.

    :param dag_name: the name of the DAG
    :param dag_config: a dictionary containing configuration for the DAG
    :param default_config: a dictitionary containing defaults for all DAGs
        in the YAML file
    """

    def __init__(
        self, dag_name: str, dag_config: Dict[str, Any], default_config: Dict[str, Any]
    ) -> None:
        self.dag_name: str = dag_name
        self.dag_config: Dict[str, Any] = dag_config
        self.default_config: Dict[str, Any] = default_config

    def get_dag_params(self) -> Dict[str, Any]:
        """
        Merges default config with dag config, sets dag_id, and extropolates dag_start_date

        :returns: dict of dag parameters
        """
        try:
            dag_params: Dict[str, Any] = utils.merge_configs(
                self.dag_config, self.default_config
            )
        except Exception as err:
            raise Exception("Failed to merge config with default config") from err
        dag_params["dag_id"]: str = self.dag_name

        if dag_params.get("task_groups") and version.parse(
            AIRFLOW_VERSION
        ) < version.parse("2.0.0"):
            raise Exception("`task_groups` key can only be used with Airflow 2.x.x")

        if (
            utils.check_dict_key(dag_params, "schedule_interval")
            and dag_params["schedule_interval"] == "None"
        ):
            dag_params["schedule_interval"] = None

        # Convert from 'dagrun_timeout_sec: int' to 'dagrun_timeout: timedelta'
        if utils.check_dict_key(dag_params, "dagrun_timeout_sec"):
            dag_params["dagrun_timeout"]: timedelta = timedelta(
                seconds=dag_params["dagrun_timeout_sec"]
            )
            del dag_params["dagrun_timeout_sec"]

        # Convert from 'end_date: Union[str, datetime, date]' to 'end_date: datetime'
        if utils.check_dict_key(dag_params["default_args"], "end_date"):
            dag_params["default_args"]["end_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["end_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )

        if utils.check_dict_key(dag_params, "custom_callbacks"):
            dag_params = utils.set_callback_from_custom(dag_params)

        if utils.check_dict_key(dag_params["default_args"], "retry_delay_sec"):
            dag_params["default_args"]["retry_delay"]: timedelta = timedelta(
                seconds=dag_params["default_args"]["retry_delay_sec"]
            )
            del dag_params["default_args"]["retry_delay_sec"]

        if utils.check_dict_key(
            dag_params, "on_success_callback_name"
        ) and utils.check_dict_key(dag_params, "on_success_callback_file"):
            dag_params["on_success_callback"]: Callable = utils.get_python_callable(
                dag_params["on_success_callback_name"],
                dag_params["on_success_callback_file"],
            )

        if utils.check_dict_key(
            dag_params, "on_failure_callback_name"
        ) and utils.check_dict_key(dag_params, "on_failure_callback_file"):
            dag_params["on_failure_callback"]: Callable = utils.get_python_callable(
                dag_params["on_failure_callback_name"],
                dag_params["on_failure_callback_file"],
            )

        try:
            # ensure that default_args dictionary contains key "start_date"
            # with "datetime" value in specified timezone
            dag_params["default_args"]["start_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["start_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )
        except KeyError as err:
            raise Exception(f"{self.dag_name} config is missing start_date") from err
        return dag_params

    @staticmethod
    def make_task(operator: str, task_params: Dict[str, Any]) -> BaseOperator:
        """
        Takes an operator and params and creates an instance of that operator.

        :returns: instance of operator object
        """
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            operator_obj: Callable[..., BaseOperator] = import_string(operator)
        except Exception as err:
            raise Exception(f"Failed to import operator: {operator}") from err
        try:
            if operator_obj == PythonOperator:
                if not task_params.get("python_callable_name") and not task_params.get(
                    "python_callable_file"
                ):
                    raise Exception(
                        "Failed to create task. PythonOperator requires `python_callable_name` \
                        and `python_callable_file` parameters."
                    )
                task_params["python_callable"]: Callable = utils.get_python_callable(
                    task_params["python_callable_name"],
                    task_params["python_callable_file"],
                )
                # remove dag-factory specific parameters
                # Airflow 2.0 doesn't allow these to be passed to operator
                del task_params["python_callable_name"]
                del task_params["python_callable_file"]

            # KubernetesPodOperator
            if operator_obj == KubernetesPodOperator:
                task_params["secrets"] = (
                    [Secret(**v) for v in task_params.get("secrets")]
                    if task_params.get("secrets") is not None
                    else None
                )

                task_params["ports"] = (
                    [Port(**v) for v in task_params.get("ports")]
                    if task_params.get("ports") is not None
                    else None
                )
                task_params["volume_mounts"] = (
                    [VolumeMount(**v) for v in task_params.get("volume_mounts")]
                    if task_params.get("volume_mounts") is not None
                    else None
                )
                task_params["volumes"] = (
                    [Volume(**v) for v in task_params.get("volumes")]
                    if task_params.get("volumes") is not None
                    else None
                )
                task_params["pod_runtime_info_envs"] = (
                    [
                        PodRuntimeInfoEnv(**v)
                        for v in task_params.get("pod_runtime_info_envs")
                    ]
                    if task_params.get("pod_runtime_info_envs") is not None
                    else None
                )
                task_params["full_pod_spec"] = (
                    V1Pod(**task_params.get("full_pod_spec"))
                    if task_params.get("full_pod_spec") is not None
                    else None
                )
                task_params["init_containers"] = (
                    [V1Container(**v) for v in task_params.get("init_containers")]
                    if task_params.get("init_containers") is not None
                    else None
                )

            if utils.check_dict_key(task_params, "execution_timeout_secs"):
                task_params["execution_timeout"]: timedelta = timedelta(
                    seconds=task_params["execution_timeout_secs"]
                )
                del task_params["execution_timeout_secs"]

            if utils.check_dict_key(task_params, "execution_delta_secs"):
                task_params["execution_delta"]: timedelta = timedelta(
                    seconds=task_params["execution_delta_secs"]
                )
                del task_params["execution_delta_secs"]

            if utils.check_dict_key(
                task_params, "execution_date_fn_name"
            ) and utils.check_dict_key(task_params, "execution_date_fn_file"):
                task_params["execution_date_fn"]: Callable = utils.get_python_callable(
                    task_params["execution_date_fn_name"],
                    task_params["execution_date_fn_file"],
                )
                del task_params["execution_date_fn_name"]
                del task_params["execution_date_fn_file"]

            # use variables as arguments on operator
            if utils.check_dict_key(task_params, "variables_as_arguments"):
                variables: List[Dict[str, str]] = task_params.get(
                    "variables_as_arguments"
                )
                for variable in variables:
                    if Variable.get(variable["variable"], default_var=None) is not None:
                        task_params[variable["attribute"]] = Variable.get(
                            variable["variable"], default_var=None
                        )
                del task_params["variables_as_arguments"]

            if utils.check_dict_key(task_params, "container_spec"):
                task_params["executor_config"] = {
                    "pod_override": V1Pod(
                        spec=V1PodSpec(
                            containers=[V1Container(**task_params["container_spec"])]
                        )
                    )
                }
                del task_params["container_spec"]

            if utils.check_dict_key(task_params, "custom_callbacks"):
                task_params = utils.set_callback_from_custom(task_params)
                del task_params["custom_callbacks"]

            task: BaseOperator = operator_obj(**task_params)
        except Exception as err:
            raise Exception(f"Failed to create {operator_obj} task") from err
        return task

    @staticmethod
    def make_task_groups(
        task_groups: Dict[str, Any], dag: DAG
    ) -> Dict[str, "TaskGroup"]:
        """Takes a DAG and task group configurations. Creates TaskGroup instances.

        :param task_groups: Task group configuration from the YAML configuration file.
        :param dag: DAG instance that task groups to be added.
        """
        task_groups_dict: Dict[str, "TaskGroup"] = {}
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
            for task_group_name, task_group_conf in task_groups.items():
                task_group_conf["group_id"] = task_group_name
                task_group_conf["dag"] = dag
                task_group = TaskGroup(
                    **{
                        k: v
                        for k, v in task_group_conf.items()
                        if k not in SYSTEM_PARAMS
                    }
                )
                task_groups_dict[task_group.group_id] = task_group
        return task_groups_dict

    @staticmethod
    def set_dependencies(
        tasks_config: Dict[str, Dict[str, Any]],
        operators_dict: Dict[str, BaseOperator],
        task_groups_config: Dict[str, Dict[str, Any]],
        task_groups_dict: Dict[str, "TaskGroup"],
    ):
        """Take the task configurations in YAML file and operator
        instances, then set the dependencies between tasks.

        :param tasks_config: Raw task configuration from YAML file
        :param operators_dict: Dictionary for operator instances
        :param task_groups_config: Raw task group configuration from YAML file
        :param task_groups_dict: Dictionary for task group instances
        """
        tasks_and_task_groups_config = {**tasks_config, **task_groups_config}
        tasks_and_task_groups_instances = {**operators_dict, **task_groups_dict}
        for name, conf in tasks_and_task_groups_config.items():
            # if task is in a task group, group_id is prepended to its name
            if conf.get("task_group"):
                group_id = conf["task_group"].group_id
                name = f"{group_id}.{name}"
            if conf.get("dependencies"):
                source: Union[
                    BaseOperator, "TaskGroup"
                ] = tasks_and_task_groups_instances[name]
                for dep in conf["dependencies"]:
                    if tasks_and_task_groups_config[dep].get("task_group"):
                        group_id = tasks_and_task_groups_config[dep][
                            "task_group"
                        ].group_id
                        dep = f"{group_id}.{dep}"
                    dep: Union[
                        BaseOperator, "TaskGroup"
                    ] = tasks_and_task_groups_instances[dep]
                    source.set_upstream(dep)

    def build_fallback_dag(self, error: str) -> Dict[str, DAG]:
        """
        Build a rudimentary DAG without tasks
        to inform errors without breaking the flow of dag-factory
        """
        dag_id = f"ERROR_{self.dag_name}"
        dag: DAG = DAG(
            dag_id=dag_id, start_date=datetime(2030, 1, 1), doc_md=str(error)
        )

        return {"dag_id": dag_id, "dag": dag}

    def build(self) -> Dict[str, DAG]:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_params: Dict[str, Any] = self.get_dag_params()
        dag: DAG = DAG(
            dag_id=dag_params["dag_id"],
            schedule_interval=dag_params.get("schedule_interval", timedelta(days=1)),
            description=(
                dag_params.get("description", None)
                if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.11")
                else dag_params.get("description", "")
            ),
            concurrency=dag_params.get(
                "concurrency",
                configuration.conf.getint("core", "max_active_tasks_per_dag"),
            ),
            catchup=dag_params.get(
                "catchup",
                configuration.conf.getboolean("scheduler", "catchup_by_default"),
            ),
            max_active_runs=dag_params.get(
                "max_active_runs",
                configuration.conf.getint("core", "max_active_runs_per_dag"),
            ),
            dagrun_timeout=dag_params.get("dagrun_timeout", None),
            default_view=dag_params.get(
                "default_view", configuration.conf.get("webserver", "dag_default_view")
            ),
            orientation=dag_params.get(
                "orientation",
                configuration.conf.get("webserver", "dag_orientation"),
            ),
            on_success_callback=dag_params.get("on_success_callback", None),
            on_failure_callback=dag_params.get("on_failure_callback", None),
            default_args=dag_params.get("default_args", None),
            doc_md=dag_params.get("doc_md", None),
        )

        if dag_params.get("doc_md_file_path"):
            if not os.path.isabs(dag_params.get("doc_md_file_path")):
                raise Exception("`doc_md_file_path` must be absolute path")

            with open(dag_params.get("doc_md_file_path"), "r") as file:
                dag.doc_md = file.read()

        if dag_params.get("doc_md_python_callable_file") and dag_params.get(
            "doc_md_python_callable_name"
        ):
            doc_md_callable = utils.get_python_callable(
                dag_params.get("doc_md_python_callable_name"),
                dag_params.get("doc_md_python_callable_file"),
            )
            dag.doc_md = doc_md_callable(
                **dag_params.get("doc_md_python_arguments", {})
            )

        # tags parameter introduced in Airflow 1.10.8
        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
            dag.tags = dag_params.get("tags", None)

        tasks: Dict[str, Dict[str, Any]] = dag_params["tasks"]

        # add a property to mark this dag as an auto-generated on
        dag.is_dagfactory_auto_generated = True

        # create dictionary of task groups
        task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(
            dag_params.get("task_groups", {}), dag
        )

        # create dictionary to track tasks and set dependencies
        tasks_dict: Dict[str, BaseOperator] = {}
        for task_name, task_conf in tasks.items():
            if utils.check_dict_key(dag_params, "custom_callbacks"):
                task_conf["custom_callbacks"] = dag_params["custom_callbacks"]
                del dag_params["custom_callbacks"]

            task_conf["task_id"]: str = task_name
            task_conf["dag"]: DAG = dag
            if "operator" in task_conf:
                operator: str = task_conf["operator"]
                # add task to task_group
                if task_groups_dict and task_conf.get("task_group_name"):
                    task_conf["task_group"] = task_groups_dict[
                        task_conf.get("task_group_name")
                    ]
                params: Dict[str, Any] = {
                    k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS
                }
                task: BaseOperator = DagBuilder.make_task(
                    operator=operator, task_params=params
                )
                tasks_dict[task.task_id]: BaseOperator = task
            elif "generator" in task_conf:
                generator: str = task_conf["generator"]

                # add task to task_group
                if task_groups_dict and task_conf.get("task_group_name"):
                    task_conf["task_group"] = task_groups_dict[
                        task_conf.get("task_group_name")
                    ]
                params: Dict[str, Any] = {
                    k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS
                }
                class_name = generator.split(".")[-1]
                module_path = generator.replace(f".{class_name}", "")
                module = importlib.import_module(module_path)

                instance = getattr(module, class_name)(self, params)
                generated_tasks = instance.generate_tasks(params, task_conf)
                if not generated_tasks:
                    # when tasks are not generated, DAG should not be created
                    return dict()
                tasks_dict.update(generated_tasks)
            else:
                raise Exception(
                    f"`{task_name}` has no 'operator' neither 'generator' specified"
                )

        # set task dependencies after creating tasks
        self.set_dependencies(
            tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict
        )

        return {"dag_id": dag_params["dag_id"], "dag": dag}
