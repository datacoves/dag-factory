from typing import Any, Dict, Set

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from slugify import slugify

from .base import BaseGenerator

FIVETRAN_API_BASE_URL = "https://api.fivetran.com/v1"
API_ENDPOINTS = {
    "GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups",
    "GROUP_DETAIL": FIVETRAN_API_BASE_URL + "/destinations/{group}",
    "CONNECTOR_GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups/{group}/connectors",
    "CONNECTOR_DETAILS": FIVETRAN_API_BASE_URL + "/connectors/{connector}",
}


class FivetranGeneratorException(Exception):
    pass


class FivetranGenerator(BaseGenerator):
    def __init__(self, dag_builder, params):
        self.dag_builder = dag_builder

        try:
            fivetran_connection_name = params["airflow_connection_id"]
            self.fivetran_connection = BaseHook.get_connection(fivetran_connection_name)

        except KeyError:
            raise FivetranGeneratorException(
                "`airflow_connection_id` is missing in Fivetran DAG configuration YAML file"
            )
        (
            self.fivetran_connectors_set,
            self.fivetran_data,
        ) = self._populate_fivetran_data()

    def _fivetran_api_call(self, method: str, endpoint: str):
        """
        Common method to reach Fivetran API, extensible for future Methods and Endpoints
        """
        fivetran_auth = (
            self.fivetran_connection.login,
            self.fivetran_connection.password,
        )
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
        fivetran_groups = self._fivetran_api_call(
            "GET", API_ENDPOINTS.get("GROUP_LIST")
        )
        for group in fivetran_groups.get("data", {}).get("items", []):
            group_data = {}
            group_id = group["id"]
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
            "GET", API_ENDPOINTS["GROUP_DETAIL"].format(group=group_id)
        )
        return group_details.get("data")

    def _get_connector_details(self, connector_id):
        """
        Get Connector details from Fivetran API
        """
        connector_details = self._fivetran_api_call(
            "GET", API_ENDPOINTS["CONNECTOR_DETAILS"].format(connector=connector_id)
        )
        return connector_details.get("data")

    def _get_group_connectors(
        self, fivetran_connectors: Set[str], group_id: str
    ) -> Dict[Any, Any]:
        """
        Get Group connectors (ids and their details)
        """
        group_connectors = self._fivetran_api_call(
            "GET", API_ENDPOINTS["CONNECTOR_GROUP_LIST"].format(group=group_id)
        )
        connector_data = {}
        for connector in group_connectors.get("data", {}).get("items", []):
            connector_id = connector["id"]
            fivetran_connectors.add(connector_id)
            connector_data[connector_id] = self._get_connector_details(connector_id)
        return connector_data

    def _get_fivetran_connector_name_for_id(self, connector_id):
        """
        Create a name for Fivetran tasks based on a Connector ID
        """
        for dest_dict in self.fivetran_data.values():
            for conn_key, conn_dict in dest_dict.get("connectors", {}).items():
                if conn_key == connector_id:
                    return slugify(f"{conn_key} → {conn_dict['schema']}")

    def remove_inexistant_connector_ids(self, connectors_ids: Set[str]) -> Set[str]:
        """
        Ensure no invalid Connector ID was passed to the Generator
        - Clean the given set of those IDs that don't exist in Fivetran
        """
        return {id for id in connectors_ids if id in self.fivetran_connectors_set}

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
            tasks[task_id] = self.generate_sync_task(params, FivetranOperator)
        return tasks

    def check_value_in_object(self, obj, value):
        if isinstance(obj, str):
            if value in obj.lower():
                return True
        elif isinstance(obj, dict):
            for v in obj.values():
                if self.check_value_in_object(v, value):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if self.check_value_in_object(item, value):
                    return True
        else:
            return False
        return False

    def get_pipeline_connection_id(
        self, source_db: str, source_schema: str, source_table: str
    ) -> str:
        """
        Given a table name, schema and db, returns the corresponding Fivetran Connection ID
        """
        fivetran_schema_db_naming = f"{source_schema}.{source_table}".lower()
        for dest_dict in self.fivetran_data.values():
            # destination dict can be empty if Fivetran Destination is missing configuration or not yet tested
            if dest_dict and dest_dict.get("details"):
                # match dbt source_db to Fivetran destination database
                if self.check_value_in_object(dest_dict, source_db.lower()):
                    # find the appropiate Connector from destination connectors)
                    for connector_dict in dest_dict.get("connectors").values():
                        if self.check_value_in_object(
                            connector_dict, source_schema.lower()
                        ) and self.check_value_in_object(
                            connector_dict, source_table.lower()
                        ):
                            return connector_dict["id"]
        raise FivetranGeneratorException(
            f"There is no Fivetran Connector for {source_db}.{fivetran_schema_db_naming}"
        )


class FivetranDbtGenerator(FivetranGenerator, BaseGenerator):
    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["connections_ids"] = self.get_pipeline_connection_list(params)
        return super().generate_tasks(params)
