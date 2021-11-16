import os
import datetime
from unittest.mock import patch

import pendulum
import pytest
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import __version__ as AIRFLOW_VERSION
from packaging import version

from dagfactory import dagbuilder

here = os.path.dirname(__file__)


DEFAULT_CONFIG = {
    "default_args": {
        "owner": "default_owner",
        "start_date": datetime.date(2018, 3, 1),
        "end_date": datetime.date(2018, 3, 5),
        "retries": 1,
        "retry_delay_sec": 300,
    },
    "concurrency": 1,
    "max_active_runs": 1,
    "dagrun_timeout_sec": 600,
    "schedule_interval": "0 1 * * *",
}
DAG_CONFIG = {
    "doc_md": "##here is a doc md string",
    "default_args": {"owner": "custom_owner"},
    "description": "this is an example dag",
    "schedule_interval": "0 3 * * *",
    "tags": ["tag1", "tag2"],
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
            "execution_timeout_secs": 5,
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "dependencies": ["task_1"],
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "dependencies": ["task_1"],
        },
    },
}
DAG_CONFIG_TASK_GROUP = {
    "default_args": {"owner": "custom_owner"},
    "schedule_interval": "0 3 * * *",
    "task_groups": {
        "task_group_1": {
            "tooltip": "this is a task group",
            "dependencies": ["task_1"],
        },
        "task_group_2": {
            "dependencies": ["task_group_1"],
        },
        "task_group_3": {},
    },
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "task_group_name": "task_group_1",
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "task_group_name": "task_group_1",
            "dependencies": ["task_2"],
        },
        "task_4": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 4",
            "dependencies": ["task_group_1"],
        },
        "task_5": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 5",
            "task_group_name": "task_group_2",
        },
        "task_6": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 6",
            "task_group_name": "task_group_2",
            "dependencies": ["task_5"],
        },
    },
}
UTC = pendulum.timezone("UTC")


class MockTaskGroup:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_get_dag_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    expected = {
        "doc_md": "##here is a doc md string",
        "dag_id": "test_dag",
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.datetime(2018, 3, 1, 0, 0, tzinfo=UTC),
            "end_date": datetime.datetime(2018, 3, 5, 0, 0, tzinfo=UTC),
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=300),
        },
        "description": "this is an example dag",
        "schedule_interval": "0 3 * * *",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout": datetime.timedelta(seconds=600),
        "tags": ["tag1", "tag2"],
        "tasks": {
            "task_1": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 1",
                "execution_timeout_secs": 5,
            },
            "task_2": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 2",
                "dependencies": ["task_1"],
            },
            "task_3": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 3",
                "dependencies": ["task_1"],
            },
        },
    }
    actual = td.get_dag_params()
    assert actual == expected


def test_get_dag_params_no_start_date():
    td = dagbuilder.DagBuilder("test_dag", {}, {})
    with pytest.raises(Exception):
        td.get_dag_params()


def test_make_task_valid():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.bash_operator.BashOperator"
    task_params = {
        "task_id": "test_task",
        "bash_command": "echo 1",
        "execution_timeout_secs": 5,
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert actual.bash_command == "echo 1"
    assert isinstance(actual, BashOperator)


def test_make_task_bad_operator():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "not_real"
    task_params = {"task_id": "test_task", "bash_command": "echo 1"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_make_task_missing_required_param():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.bash_operator.BashOperator"
    task_params = {"task_id": "test_task"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def print_test():
    print("test")


def test_make_python_operator():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {
        "task_id": "test_task",
        "python_callable_name": "print_test",
        "python_callable_file": os.path.realpath(__file__),
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.python_callable)
    assert isinstance(actual, PythonOperator)


def test_make_python_operator_missing_param():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {"task_id": "test_task", "python_callable_name": "print_test"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_build():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    actual = td.build()
    assert actual["dag_id"] == "test_dag"
    assert isinstance(actual["dag"], DAG)
    assert len(actual["dag"].tasks) == 3
    assert actual["dag"].task_dict["task_1"].downstream_task_ids == {"task_2", "task_3"}
    if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
        assert actual["dag"].tags == ["tag1", "tag2"]


def test_get_dag_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP, DEFAULT_CONFIG)
    expected = {
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.datetime(2018, 3, 1, 0, 0, tzinfo=UTC),
            "end_date": datetime.datetime(2018, 3, 5, 0, 0, tzinfo=UTC),
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=300),
        },
        "schedule_interval": "0 3 * * *",
        "task_groups": {
            "task_group_1": {
                "tooltip": "this is a task group",
                "dependencies": ["task_1"],
            },
            "task_group_2": {"dependencies": ["task_group_1"]},
            "task_group_3": {},
        },
        "tasks": {
            "task_1": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 1",
            },
            "task_2": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 2",
                "task_group_name": "task_group_1",
            },
            "task_3": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 3",
                "task_group_name": "task_group_1",
                "dependencies": ["task_2"],
            },
            "task_4": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 4",
                "dependencies": ["task_group_1"],
            },
            "task_5": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 5",
                "task_group_name": "task_group_2",
            },
            "task_6": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 6",
                "task_group_name": "task_group_2",
                "dependencies": ["task_5"],
            },
        },
        "concurrency": 1,
        "max_active_runs": 1,
        "dag_id": "test_dag",
        "dagrun_timeout": datetime.timedelta(seconds=600),
    }
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        error_message = "`task_groups` key can only be used with Airflow 2.x.x"
        with pytest.raises(Exception, match=error_message):
            td.get_dag_params()
    else:
        assert td.get_dag_params() == expected


def test_build_task_groups():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP, DEFAULT_CONFIG)
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        error_message = "`task_groups` key can only be used with Airflow 2.x.x"
        with pytest.raises(Exception, match=error_message):
            td.build()
    else:
        actual = td.build()
        task_group_1 = {
            t for t in actual["dag"].task_dict if t.startswith("task_group_1")
        }
        task_group_2 = {
            t for t in actual["dag"].task_dict if t.startswith("task_group_2")
        }
        assert actual["dag_id"] == "test_dag"
        assert isinstance(actual["dag"], DAG)
        assert len(actual["dag"].tasks) == 6
        assert actual["dag"].task_dict["task_1"].downstream_task_ids == {
            "task_group_1.task_2"
        }
        assert actual["dag"].task_dict["task_group_1.task_2"].downstream_task_ids == {
            "task_group_1.task_3"
        }
        assert actual["dag"].task_dict["task_group_1.task_3"].downstream_task_ids == {
            "task_4",
            "task_group_2.task_5",
        }
        assert actual["dag"].task_dict["task_group_2.task_5"].downstream_task_ids == {
            "task_group_2.task_6",
        }
        assert {"task_group_1.task_2", "task_group_1.task_3"} == task_group_1
        assert {"task_group_2.task_5", "task_group_2.task_6"} == task_group_2


@patch("dagfactory.dagbuilder.TaskGroup", new=MockTaskGroup)
def test_make_task_groups():
    task_group_dict = {
        "task_group": {
            "tooltip": "this is a task group",
        },
    }
    dag = "dag"
    task_groups = dagbuilder.DagBuilder.make_task_groups(task_group_dict, dag)
    expected = MockTaskGroup(
        tooltip="this is a task group", group_id="task_group", dag=dag
    )
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        assert task_groups == {}
    else:
        assert task_groups["task_group"].__dict__ == expected.__dict__


def test_make_task_groups_empty():
    task_groups = dagbuilder.DagBuilder.make_task_groups({}, None)
    assert task_groups == {}
