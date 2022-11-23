from os import environ
from typing import Any, Dict

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from slugify import slugify

from .base import BaseGenerator

TEST_MODE = bool(environ.get("TEST_MODE"))


class AirbyteGeneratorException(Exception):
    pass


class AirbyteGenerator(BaseGenerator):
    def __init__(self, dag_builder, params):
        self.dag_builder = dag_builder

        try:
            airbyte_connection_name = params["airflow_connection_id"]

        except KeyError:
            raise AirbyteGeneratorException(
                "`airflow_connection_id` is missing in Airbyte DAG configuration YAML file"
            )

        if TEST_MODE:
            self.airbyte_connections = []
            self.connections_should_exist = False
        else:
            airbyte_connection = BaseHook.get_connection(airbyte_connection_name)

            airbyte_api_url = (
                f"http://{airbyte_connection.host}:{airbyte_connection.port}/api/v1/"
            )
            airbyte_api_endpoint_list_entity = airbyte_api_url + "{entity}/list"
            airbyte_api_endpoint_list_workspaces = (
                airbyte_api_endpoint_list_entity.format(entity="workspaces")
            )
            airbyte_api_endpoint_list_connections = (
                airbyte_api_endpoint_list_entity.format(entity="connections")
            )
            airbyte_api_endpoint_list_destinations = (
                airbyte_api_endpoint_list_entity.format(entity="destinations")
            )
            airbyte_api_endpoint_list_sources = airbyte_api_endpoint_list_entity.format(
                entity="sources"
            )
            self.airbyte_workspace_id = self.api_call(
                "POST",
                airbyte_api_endpoint_list_workspaces,
            )["workspaces"][0]["workspaceId"]
            self.airbyte_api_standard_req_body = {
                "workspaceId": self.airbyte_workspace_id
            }
            self.airbyte_connections = self.api_call(
                "POST",
                airbyte_api_endpoint_list_connections,
                self.airbyte_api_standard_req_body,
            )["connections"]
            self.airbyte_destinations = self.api_call(
                "POST",
                airbyte_api_endpoint_list_destinations,
                self.airbyte_api_standard_req_body,
            )["destinations"]
            self.airbyte_sources = self.api_call(
                "POST",
                airbyte_api_endpoint_list_sources,
                self.airbyte_api_standard_req_body,
            )["sources"]
            self.connections_should_exist = True

    def remove_inexistant_conn_ids(self, connections_ids):
        """
        Overwrite the user-written `connections_ids` set (.yml) with the ones
        actually existing inside Airbyte
        If not handled, a lot of Airflow tasks would fail.
        """
        return {
            c_id
            for c_id in connections_ids
            if c_id
            in [connection["connectionId"] for connection in self.airbyte_connections]
        }

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        AirbyteGenerator entry-point from both `dagbuilder`
            Clean params dictionary from information that Airflow can't handle
            Fill and return the tasks dictionary
        """
        params["airbyte_conn_id"] = params.pop("airflow_connection_id")
        connections_ids = set(params.pop("connections_ids"))

        generator_class = params.pop("generator")
        if "AirbyteGenerator" in generator_class:
            connections_ids = self.remove_inexistant_conn_ids(connections_ids)

        tasks: Dict[str, BaseOperator] = dict()
        for conn_id in connections_ids:
            task_id = self._create_airbyte_connection_name_for_id(conn_id)
            params["task_id"] = task_id
            params["connection_id"] = conn_id
            tasks[task_id] = self.generate_sync_task(params, AirbyteTriggerSyncOperator)

        return tasks

    def _get_airbyte_destination(self, id):
        """Given a destination id, returns the destination payload"""
        for destination in self.airbyte_destinations:
            if destination["destinationId"] == id:
                return destination
        raise AirbyteGeneratorException(
            f"Airbyte error: there are no destinations for id {id}"
        )

    def _get_airbyte_source(self, id):
        """Get the complete Source object from it's ID"""
        for source in self.airbyte_sources:
            if source["sourceId"] == id:
                return source
        raise AirbyteGeneratorException(
            f"Airbyte extract error: there is no Airbyte Source for id [red]{id}[/red]"
        )

    def _get_connection_schema(self, conn, destination_config):
        """Given an airybte connection, returns a schema name"""
        namespace_definition = conn["namespaceDefinition"]

        if namespace_definition == "source" or (
            conn["namespaceDefinition"] == "customformat"
            and conn["namespaceFormat"] == "${SOURCE_NAMESPACE}"
        ):
            source = self._get_airbyte_source(conn["sourceId"])
            if "schema" in source["connectionConfiguration"]:
                return source["connectionConfiguration"]["schema"].lower()
            else:
                return None
        elif namespace_definition == "destination":
            return destination_config["schema"].lower()
        else:
            if namespace_definition == "customformat":
                return conn["namespaceFormat"].lower()

    def get_pipeline_connection_id(self, db: str, schema: str, table: str) -> str:
        """
        Given a table name, schema and db, returns the corresponding Airbyte Connection ID
        """

        airbyte_tables = []
        for conn in self.airbyte_connections:
            for stream in conn["syncCatalog"]["streams"]:
                # look for the table
                airbyte_table = stream["stream"]["name"].lower()
                airbyte_tables.append(airbyte_table)
                if airbyte_table == table.replace("_airbyte_raw_", ""):
                    destination_config = self._get_airbyte_destination(
                        conn["destinationId"]
                    )["connectionConfiguration"]

                    # match database
                    if db == destination_config["database"].lower():
                        airbyte_schema = self._get_connection_schema(
                            conn, destination_config
                        )
                        # and finally, match schema, if defined
                        if airbyte_schema == schema or not airbyte_schema:
                            return conn.get("connectionId")
        if self.connections_should_exist:
            raise AirbyteGeneratorException(
                f"Airbyte error: there are no connections for table {db}.{schema}.{table}. "
                f"Tables checked: {', '.join(airbyte_tables)}"
            )

    def _create_airbyte_connection_name_for_id(self, conn_id):
        """
        Given a ConnectionID, create it's name using both Source and Destination ones
        """
        for conn in self.airbyte_connections:
            if conn["connectionId"] == conn_id:
                source_name = self._get_airbyte_source(conn["sourceId"])["name"]
                destination_name = self._get_airbyte_destination(conn["destinationId"])[
                    "name"
                ]
                return slugify(f"{source_name} → {destination_name}")

        raise AirbyteGeneratorException(
            f"Airbyte error: there are missing names for connection ID {conn_id}"
        )


class AirbyteDbtGenerator(AirbyteGenerator):
    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        DAG_GENERATION_TIMEOUT = 300  # 5 minutes
        params["connections_ids"] = self.get_pipeline_connection_list(params)
        return super().generate_tasks(params)
