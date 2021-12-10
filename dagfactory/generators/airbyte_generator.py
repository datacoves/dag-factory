from typing import Any, Dict
from datetime import datetime, timedelta
import subprocess, requests, json
from slugify import slugify
from pathlib import Path
from airflow.models import BaseOperator
from airflow.models.base import Base
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.hooks.base import BaseHook
from requests.exceptions import RequestException


class AirbyteGeneratorException(Exception):
    pass


class AirbyteGenerator:
    def __init__(self, dag_builder, params):
        self.dag_builder = dag_builder
        self.AIRFLOW_OPERATOR_FULL_PATH = (
            AirbyteTriggerSyncOperator.__module__
            + "."
            + AirbyteTriggerSyncOperator.__qualname__
        )
        self.AIRBYTE_DESTINATION_TABLE_PREFIX = "_airbyte_raw_"

        try:
            airbyte_connection_name = params["airflow_connection_id"]
            airbyte_connection = BaseHook.get_connection(airbyte_connection_name)
        except KeyError:
            raise AirbyteGeneratorException(
                "`airflow_connection_id` is missing in DAG's configuration YAML file"
            )

        airbyte_api_url = (
            f"http://{airbyte_connection.host}:{airbyte_connection.port}/api/v1/"
        )
        airbyte_api_endpoint_list_entity = airbyte_api_url + "{entity}/list"
        airbyte_api_endpoint_list_workspaces = airbyte_api_endpoint_list_entity.format(
            entity="workspaces"
        )
        airbyte_api_endpoint_list_connections = airbyte_api_endpoint_list_entity.format(
            entity="connections"
        )
        airbyte_api_endpoint_list_destinations = (
            airbyte_api_endpoint_list_entity.format(entity="destinations")
        )
        airbyte_api_endpoint_list_sources = airbyte_api_endpoint_list_entity.format(
            entity="sources"
        )

        self.airbyte_workspace_id = self.airbyte_api_call(
            airbyte_api_endpoint_list_workspaces,
        )["workspaces"][0]["workspaceId"]
        self.airbyte_api_standard_req_body = {"workspaceId": self.airbyte_workspace_id}
        self.airbyte_connections = self.airbyte_api_call(
            airbyte_api_endpoint_list_connections,
            self.airbyte_api_standard_req_body,
        )["connections"]
        self.airbyte_destinations = self.airbyte_api_call(
            airbyte_api_endpoint_list_destinations,
            self.airbyte_api_standard_req_body,
        )["destinations"]
        self.airbyte_sources = self.airbyte_api_call(
            airbyte_api_endpoint_list_sources,
            self.airbyte_api_standard_req_body,
        )["sources"]

    def airbyte_api_call(self, endpoint: str, body: Dict[str, str] = None):
        """
        Generic `api caller` for contacting Airbyte
        """
        try:
            response = requests.post(endpoint, json=body)

            if response.status_code == 200:
                return json.loads(response.text)
            else:
                raise RequestException(
                    f"Unexpected status code from airbyte: {response.status_code}"
                )
        except RequestException as e:
            raise AirbyteGeneratorException("Airbyte API error: " + e)

    def generate_sync_task(
        self, params: Dict[str, Any], operator: str
    ) -> Dict[str, Any]:
        """
        Standard Airflow call to `make_task`
            same logic as dagbuilder.py:
                this re-usage is why params(dict) must be cleaned up from any
                extra information that the corresponding operator can't handle
        """
        return self.dag_builder.make_task(
            operator=operator,
            task_params=params,
        )

    def remove_inexistant_conn_ids(self, connections_ids):
        """
        Overwrite the user-written `connections_ids` list (.yml) with the ones
        actually existing inside Airbyte
        If not handled, a lot of Airflow tasks would fail.
        """
        return [
            c_id
            for c_id in connections_ids
            if c_id
            in [connection["connectionId"] for connection in self.airbyte_connections]
        ]

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        AirbyteGenerator entry-point from both `dagbuilder`
            Clean params dictionary from information that Airflow can't handle
            Fill and return the tasks dictionary
        """
        params["airbyte_conn_id"] = params.pop("airflow_connection_id")
        connections_ids = params.pop("connections_ids")

        generator_class = params.pop("generator")
        if "AirbyteGenerator" in generator_class:
            connections_ids = self.remove_inexistant_conn_ids(connections_ids)

        tasks: Dict[str, BaseOperator] = dict()
        for conn_id in connections_ids:
            task_id = self._create_airbyte_connection_name_for_id(conn_id)
            params["task_id"] = task_id
            params["connection_id"] = conn_id
            tasks[task_id] = self.generate_sync_task(
                params, self.AIRFLOW_OPERATOR_FULL_PATH
            )

        return tasks

    def _get_airbyte_destination(self, id):
        """
        Given a destination id, returns the destination payload
        """

        for destination in self.airbyte_destinations:
            if destination["destinationId"] == id:
                return destination["connectionConfiguration"]
        raise AirbyteGeneratorException(
            f"Airbyte error: there are no destinations for id {id}"
        )

    def _get_airbyte_destination_name(self, id):
        """
        Given a destination id, returns it's name
        """

        for destination in self.airbyte_destinations:
            if destination["destinationId"] == id:
                return destination["name"]
        raise AirbyteGeneratorException(
            f"Airbyte error: there are no destinations for id {id}"
        )

    def _get_airbyte_source(self, sourceId):
        """
        Get the complete Source object from it's ID
        """
        for source in self.airbyte_api_caller.airbyte_sources_list:
            if source["sourceId"] == sourceId:
                return source
        raise AirbyteGeneratorException(
            f"Airbyte extract error: there is no Airbyte Source for id [red]{sourceId}[/red]"
        )

    def _get_airbyte_source_name(self, id):
        """
        Given a source id, returns it's name
        """

        for source in self.airbyte_sources:
            if source["sourceId"] == id:
                return source["name"]
        raise AirbyteGeneratorException(
            f"Airbyte error: there are no sources for id {id}"
        )

    def _get_airbyte_connection_for_table(self, db, schema, table):
        """
        Given a table name, returns the corresponding airbyte connection
        """

        for conn in self.airbyte_connections:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table:
                    namespace_definition = conn["namespaceDefinition"]

                    if namespace_definition == "source" or (
                        conn["namespaceDefinition"] == "customformat"
                        and conn["namespaceFormat"] == "${SOURCE_NAMESPACE}"
                    ):
                        source = self._get_airbyte_source(conn["sourceId"])
                        if "database" in source["connectionConfiguration"]:
                            if (
                                source["connectionConfiguration"]["database"].lower()
                                == db
                            ):
                                return conn
                        else:
                            return conn
                    elif namespace_definition == "destination":
                        # compare destination-schema with arg-schema
                        destination = self._get_airbyte_destination(
                            conn["destinationId"]
                        )
                        if (
                            destination["connectionConfiguration"]["schema"].lower()
                            == schema
                        ):
                            return conn

                    else:
                        if namespace_definition == "customformat":
                            if conn["namespaceFormat"] == schema:
                                return conn
        raise AirbyteGeneratorException(
            f"Airbyte error: there are no connections for table {table}"
        )

    def _create_airbyte_connection_name_for_id(self, conn_id):
        """
        Given a ConnectionID, create it's name using both Source and Destination ones
        """
        for conn in self.airbyte_connections:
            if conn["connectionId"] == conn_id:
                source_name = self._get_airbyte_source_name(conn["sourceId"])
                destination_name = self._get_airbyte_destination_name(
                    conn["destinationId"]
                )
                return slugify(f"{source_name} → {destination_name}")

        raise AirbyteGeneratorException(
            f"Airbyte error: there are missing names for connection ID {conn_id}"
        )


class AirbyteDbtGeneratorException(Exception):
    pass


class AirbyteDbtGenerator(AirbyteGenerator):
    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        DAG_GENERATION_TIMEOUT = 300  # 5 minutes
        dbt_project_path = params.pop("dbt_project_path")
        dbt_list_args = params.pop("dbt_list_args", "")
        # This is the folder to copy project to before running dbt
        deploy_path = params.pop("deploy_path", None)
        run_dbt_deps = params.pop("run_dbt_deps", True)
        run_dbt_compile = params.pop("run_dbt_compile", True)

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

        if run_dbt_deps:
            subprocess.run(["dbt", "deps"], check=True, cwd=cwd)

        if run_dbt_compile:
            subprocess.run(["dbt", "compile"], check=True, cwd=cwd)

        # Call DBT on the specified path
        process = subprocess.run(
            ["dbt", "ls", "--resource-type", "source"] + dbt_list_args.split(),
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        stdout = process.stdout.decode()

        sources_list = [
            src.replace("source:", "source.") for src in stdout.split("\n") if src
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

            conn = self._get_airbyte_connection_for_table(
                source_db, source_schema, source_table
            )
            destination_config = self._get_airbyte_destination(conn["destinationId"])
            schema = destination_config["schema"].lower()
            if conn["namespaceDefinition"] == "customformat":
                schema = conn["namespaceFormat"]

            if (
                source_db == destination_config["database"].lower()
                and source_schema == schema
            ):
                connections_ids.append(conn["connectionId"])

        params["connections_ids"] = connections_ids

        return super().generate_tasks(params)
