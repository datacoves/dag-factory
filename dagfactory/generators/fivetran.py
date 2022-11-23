from typing import Any, Dict, Set

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from slugify import slugify

from .base import BaseGenerator


class FivetranGeneratorException(Exception):
    pass


class FivetranGenerator(BaseGenerator):
    def __init__(self, dag_builder, params):
        self.dag_builder = dag_builder
        self.FIVETRAN_OPERATOR_FULL_PATH = (
            FivetranOperator.__module__ + "." + FivetranOperator.__qualname__
        )

        FIVETRAN_API_BASE_URL = "https://api.fivetran.com/v1"
        self.API_ENDPOINTS = {
            "GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups",
            "GROUP_DETAIL": FIVETRAN_API_BASE_URL + "/destinations/{group}",
            "CONNECTOR_GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups/{group}/connectors",
            "CONNECTOR_DETAILS": FIVETRAN_API_BASE_URL + "/connectors/{connector}",
        }

        try:
            fivetran_connection_name = params["airflow_connection_id"]
            self.FIVETRAN_CONNECTION = BaseHook.get_connection(fivetran_connection_name)

        except KeyError:
            raise FivetranGeneratorException(
                "`airflow_connection_id` is missing in Fivetran DAG configuration YAML file"
            )
        self.FIVETRAN_CONNECTORS_SET, self.FIVETRAN_DATA = self._populate_fivetran_data()

    def _fivetran_api_call(self, method: str, endpoint: str):
        """
        Common method to reach Fivetran API, extensible for future Methods and Endpoints
        """
        fivetran_auth = (self.FIVETRAN_CONNECTION.login, self.FIVETRAN_CONNECTION.password)
        fivetran_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json;version=2",
        }
        return self.api_call(
            method,
            endpoint,
            headers=fivetran_headers,
            auth=fivetran_auth,
        )

    def _populate_fivetran_data(self) -> Dict[Any, Any]:
        """
        Create set with Fivetran's connectors IDs
        Create dictionary with Fivetran's destinations and connectors information
        - returns {
            destination_id: {
                details: {},
                connectors: {}
            }
            [...]
        }
        """
        fivetran_connectors = set()
        fivetran_data = {}
        fivetran_groups = self._fivetran_api_call("GET", self.API_ENDPOINTS.get("GROUP_LIST"))
        for group in fivetran_groups.get("data").get("items"):
            group_data = {}
            group_id = group.get("id")
            group_details = self._get_group_details(group_id)
            if group_details:
                group_data["details"] = group_details
                group_data["connectors"] = self._get_group_connectors(
                    fivetran_connectors, group_id
                )
            fivetran_data[group_id] = group_data
        return fivetran_connectors, fivetran_data

    def _get_group_details(self, group_id) -> Dict[Any, Any]:
        """
        Get Group details from Fivetran API
        """
        group_details = self._fivetran_api_call(
            "GET", self.API_ENDPOINTS.get("GROUP_DETAIL").format(group=group_id)
        )
        return group_details.get("data")

    def _get_group_connectors(
        self, fivetran_connectors: Set[str], group_id: str
    ) -> Dict[Any, Any]:
        """
        Get Group connectors (ids and their details)
        """
        group_connectors = self._fivetran_api_call(
            "GET", self.API_ENDPOINTS.get("CONNECTOR_GROUP_LIST").format(group=group_id)
        )
        connector_data = {}
        for connector in group_connectors.get("data").get("items"):
            connector_id = connector.get("id")
            fivetran_connectors.add(connector_id)
            connector_data[connector_id] = connector
        return connector_data

    def _get_fivetran_connector_name_for_id(self, connector_id):
        """
        Create a name for Fivetran tasks based on a Connector ID
        """
        for dest_key, dest_dict in self.FIVETRAN_DATA.items():
            if dest_dict and dest_dict.get("connectors"):
                for conn_key, conn_dict in dest_dict.get("connectors").items():
                    if conn_key == connector_id:
                        return slugify(f"{conn_key} → {conn_dict.get('schema')}")

    def remove_inexistant_connector_ids(self, connectors_ids: Set[str]) -> Set[str]:
        """
        Ensure no invalid Connector ID was passed to the Generator
        - Clean the given set of those IDs that don't exist in Fivetran
        """
        return {id for id in connectors_ids if id in self.FIVETRAN_CONNECTORS_SET}

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Common generate_tasks for both Fivetran and Fivetran DBT generators
        - Clean params dictionary from information that Airflow can't handle
        - Fill and return the tasks dictionary
        """
        params["fivetran_conn_id"] = params.pop("airflow_connection_id")
        connectors_ids = set(params.pop("connections_ids"))
        generator_class = params.pop("generator")

        if "FivetranGenerator" in generator_class:
            connectors_ids = self.remove_inexistant_connector_ids(connectors_ids)

        tasks: Dict[str, BaseOperator] = {}
        for conn_id in connectors_ids:
            task_id = self._get_fivetran_connector_name_for_id(conn_id)
            params["task_id"] = task_id
            params["connector_id"] = conn_id
            tasks[task_id] = self.generate_sync_task(params, self.FIVETRAN_OPERATOR_FULL_PATH)
        return tasks

    def get_pipeline_connection_id(
        self, source_db: str, source_schema: str, source_table: str
    ) -> str:
        """
        Given a table name, schema and db, returns the corresponding Fivetran Connection ID
        """
        fivetran_schema_db_naming = f"{source_schema}.{source_table}".lower()
        for dest_key, dest_dict in self.FIVETRAN_DATA.items():
            # destination dict can be empty if Fivetran Destination is missing configuration or not yet tested
            if dest_dict and dest_dict.get("details"):
                # match dbt source_db to Fivetran destination database
                if (
                    dest_dict.get("details").get("config").get("database").lower()
                    == source_db.lower()
                ):
                    # find the appropiate Connector from destination connectors
                    for connector_key, connector_dict in dest_dict.get("connectors").items():
                        if (
                            connector_dict.get("schema")
                            and connector_dict.get("schema").lower() == fivetran_schema_db_naming
                        ):
                            return connector_dict.get("id")
        raise FivetranGeneratorException(
            f"There is no Fivetran Connector for {source_db}.{fivetran_schema_db_naming}"
        )


class FivetranDbtGenerator(FivetranGenerator, BaseGenerator):
    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["connections_ids"] = self.get_pipeline_connection_list(params)
        return super().generate_tasks(params)
