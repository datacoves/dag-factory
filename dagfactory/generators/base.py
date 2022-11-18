import json
import shlex
import subprocess
from os import environ
from pathlib import Path
from typing import Any, Dict, List

import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from requests.exceptions import RequestException
from slugify import slugify


class GeneratorException(Exception):
    pass


class BaseGenerator:
    """
    Common functionalities for all Generators
    """

    def get_bash_command(self, virtualenv_path, command):
        return shlex.split(f"/bin/bash -c 'source {virtualenv_path} && {command}'")

    def get_pipeline_connection_list(self, params: Dict[str, Any]) -> List[str]:
        dbt_project_path = params.pop("dbt_project_path")
        dbt_list_args = params.pop("dbt_list_args", "")
        # This is the folder to copy project to before running dbt
        deploy_path = params.pop("deploy_path", None)
        run_dbt_deps = params.pop("run_dbt_deps", True)
        run_dbt_compile = params.pop("run_dbt_compile", False)
        virtualenv_path = params.pop("virtualenv_path", None)
        if virtualenv_path:
            virtualenv_path = Path(f"{virtualenv_path}/bin/activate").absolute()

        if Path(dbt_project_path).is_absolute():
            dbt_project_path = Path(dbt_project_path)
        else:
            dbt_project_path = (
                Path(environ.get("DATACOVES__REPO_PATH", "/opt/airflow/dags/repo"))
                / dbt_project_path
            )
        cwd = dbt_project_path
        if deploy_path:
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                cwd=dbt_project_path,
            ).stdout.strip("\n")
            deploy_path += "-" + commit
            # Move folders
            subprocess.run(["cp", "-rf", dbt_project_path, deploy_path], check=True)
            cwd = deploy_path

        try:
            if run_dbt_deps:
                if virtualenv_path:
                    command = self.get_bash_command(virtualenv_path, "dbt deps")
                else:
                    command = ["dbt", "deps"]
                subprocess.run(
                    command,
                    check=True,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            if run_dbt_compile:
                if virtualenv_path:
                    command = self.get_bash_command(
                        virtualenv_path, f"dbt compile {dbt_list_args}"
                    )
                else:
                    command = ["dbt", "compile"] + dbt_list_args.split()
                subprocess.run(
                    command,
                    check=True,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            if virtualenv_path:
                command = self.get_bash_command(
                    virtualenv_path, f"dbt ls --resource-type source {dbt_list_args}"
                )
            else:
                command = [
                    "dbt",
                    "ls",
                    "--resource-type",
                    "source",
                ] + dbt_list_args.split()

            process = subprocess.run(
                command,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            stdout = process.stdout.decode()
        except subprocess.CalledProcessError as e:
            error_message = ""
            if e.stdout:
                error_message += f"{e.stdout.decode()}\n"
            if e.stderr:
                error_message += f"{e.stderr.decode()}"
            raise GeneratorException(f"Exception ocurred running {command}\n{error_message}")

        sources_list = []
        if "No nodes selected" not in stdout:
            sources_list = [
                src.replace("source:", "source.")
                for src in stdout.split("\n")
                if (src and "source:" in src)
            ]
        manifest_json = json.load(open(Path(cwd) / "target" / "manifest.json"))

        if deploy_path:
            subprocess.run(["rm", "-rf", deploy_path], check=True)

        connections_ids = []
        for source in sources_list:
            # Transform the 'dbt source' into [db, schema, table]
            source_db = manifest_json["sources"][source]["database"].lower()
            source_schema = manifest_json["sources"][source]["schema"].lower()
            source_table = manifest_json["sources"][source]["identifier"].lower()

            conn = self.get_pipeline_connection(params, source_db, source_schema, source_table)

            if conn and conn["connectionId"] not in connections_ids:
                connections_ids.append(conn["connectionId"])

        params["connections_ids"] = connections_ids
