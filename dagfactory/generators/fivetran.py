from os import environ
from typing import Any, Dict, Set

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from slugify import slugify

from .base import BaseGenerator

FIVETRAN_API_BASE_URL = "https://api.fivetran.com/v1"
API_ENDPOINTS = {
    "GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups",
    "GROUP_DETAIL": FIVETRAN_API_BASE_URL + "/destinations/{group}",
    "CONNECTOR_GROUP_LIST": FIVETRAN_API_BASE_URL + "/groups/{group}/connectors",
    "CONNECTOR_SCHEMAS": FIVETRAN_API_BASE_URL + "/connectors/{connector}/schemas",
    "CONNECTOR_DETAILS": FIVETRAN_API_BASE_URL + "/connectors/{connector}",
}

TEST_MODE = bool(environ.get("TEST_MODE"))


class FivetranGeneratorException(Exception):
    pass


class FivetranGenerator(BaseGenerator):
    def __init__(self, dag_builder, params):
        self.dag_builder = dag_builder
        self.ignored_source_tables = ["fivetran_audit", "fivetran_audit_warning"]

        if TEST_MODE:
            self.fivetran_data = {}
            self.fivetran_connectors_set = set()
            self.fivetran_groups = {}
            self.connectors_should_exist = False
        else:
            try:
                fivetran_connection_name = params["airflow_connection_id"]
                self.fivetran_connection = BaseHook.get_connection(
                    fivetran_connection_name
                )

            except KeyError:
                raise FivetranGeneratorException(
                    "`airflow_connection_id` is missing in Fivetran DAG configuration YAML file"
                )
            (
                self.fivetran_connectors_set,
                self.fivetran_data,
                self.fivetran_groups,
            ) = self._populate_fivetran_data()
            self.connectors_should_exist = True

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
        fivetran_group_name_mapping = {}
        fivetran_groups = self._fivetran_api_call(
            "GET", API_ENDPOINTS.get("GROUP_LIST")
        )
        for group in fivetran_groups.get("data", {}).get("items", []):
            group_data = {}
            group_id = group["id"]
            fivetran_group_name_mapping[group_id] = group["name"]
            group_details = self._get_group_details(group_id)
            if group_details:
                group_data["details"] = group_details
                group_data["connectors"] = self._get_group_connectors(
                    fivetran_connectors, group_id
                )
            fivetran_data[group_id] = group_data
        return fivetran_connectors, fivetran_data, fivetran_group_name_mapping

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
        Get Connector schemas from Fivetran API
        """
        connector_details = self._fivetran_api_call(
            "GET", API_ENDPOINTS["CONNECTOR_DETAILS"].format(connector=connector_id)
        )
        return connector_details.get("data", {})

    def _get_connector_schemas(self, connector_id):
        """
        Get Connector schemas from Fivetran API
        """
        connector_schemas = self._fivetran_api_call(
            "GET", API_ENDPOINTS["CONNECTOR_SCHEMAS"].format(connector=connector_id)
        )
        return connector_schemas.get("data", {}).get("schemas", {})

    def _get_group_connectors(
        self, fivetran_connectors: Set[str], group_id: str
    ) -> Dict[Any, Any]:
        """
        Get Group connectors (ids and their schemas)
        """
        group_connectors = self._fivetran_api_call(
            "GET", API_ENDPOINTS["CONNECTOR_GROUP_LIST"].format(group=group_id)
        )
        connector_data = {}
        for connector in group_connectors.get("data", {}).get("items", []):
            connector_dict = {}
            connector_id = connector["id"]
            fivetran_connectors.add(connector_id)
            connector_dict["details"] = self._get_connector_details(connector_id)
            connector_dict["schemas"] = self._get_connector_schemas(connector_id)
            connector_data[connector_id] = connector_dict

        return connector_data

    def _get_fivetran_connector_name_for_id(self, connector_id):
        """
        Create a name for Fivetran tasks based on a Connector ID
        """
        for dest_data in self.fivetran_data.values():
            for connector_data in dest_data.get("connectors", {}).values():
                details = connector_data["details"]
                if details["id"] == connector_id:
                    return slugify(
                        f"{self.fivetran_groups[details['group_id']]}.{details['schema']}"
                    )

    def remove_inexistant_connector_ids(self, connectors_ids: Set[str]) -> Set[str]:
        """
        Ensure no invalid Connector ID was passed to the Generator
        - Clean the given set of those IDs that don't exist in Fivetran
        """
        return {id for id in connectors_ids if id in self.fivetran_connectors_set}

    def generate_tasks(
        self, params: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Common generate_tasks for both Fivetran and Fivetran DBT generators
        - Clean params dictionary from information that Airflow can't handle
        - Fill and return the tasks dictionary
        """
        params["fivetran_conn_id"] = params.pop("airflow_connection_id")
        wait_for_completion = params.pop("wait_for_completion", True)
        poke_interval = params.pop("poke_interval", 30)
        connectors_ids = set(params.pop("connections_ids"))
        generator_class = params.pop("generator")
        task_group = config.get("task_group")

        if "FivetranGenerator" in generator_class:
            connectors_ids = self.remove_inexistant_connector_ids(connectors_ids)

        tasks: Dict[str, BaseOperator] = {}
        for conn_id in connectors_ids:
            task_name = self._get_fivetran_connector_name_for_id(conn_id)

            # Trigger task
            trigger_params = params.copy()
            trigger_id = task_name + "-trigger"
            trigger_params["task_id"] = trigger_id
            trigger_params["connector_id"] = conn_id
            trigger_params["do_xcom_push"] = True
            trigger = self.generate_sync_task(trigger_params, FivetranOperator)
            tasks[trigger.task_id] = trigger
            if wait_for_completion:
                # Sensor task - senses Fivetran connectors status
                sensor_params = params.copy()
                sensor_params["task_id"] = task_name + "-sensor"
                sensor_params["connector_id"] = conn_id
                sensor_params["poke_interval"] = poke_interval
                sensor_params["xcom"] = (
                    "{{ task_instance.xcom_pull('"
                    + (
                        f"{task_group.group_id}.{trigger_id}"
                        if task_group
                        else trigger_id
                    )
                    + "', key='return_value') }}"
                )
                sensor = self.generate_sync_task(sensor_params, FivetranSensor)
                sensor.set_upstream(trigger)
                tasks[sensor.task_id] = sensor
        return tasks

    def _dbt_database_in_destination(self, fivetran_destination, dbt_database):
        return (
            dbt_database
            == fivetran_destination.get("details")
            .get("config", {})
            .get("database", "")
            .lower()
        )

    def _dbt_schema_table_in_connector(self, connector_schemas, dbt_schema, dbt_table):
        for schema_details in connector_schemas.values():
            if schema_details.get("name_in_destination", "").lower() == dbt_schema:
                for table_details in schema_details.get("tables", {}).values():
                    if (
                        table_details.get("name_in_destination", "").lower()
                        == dbt_table
                    ):
                        return True
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
                if self._dbt_database_in_destination(dest_dict, source_db.lower()):
                    # find the appropiate Connector from destination connectors)
                    for connector_id, connector_data in dest_dict.get(
                        "connectors", {}
                    ).items():
                        for schema_id, schema_data in connector_data.get(
                            "schemas", {}
                        ).items():
                            if self._dbt_schema_table_in_connector(
                                {schema_id: schema_data},
                                source_schema.lower(),
                                source_table.lower(),
                            ):
                                return connector_id
        if self.connectors_should_exist:
            raise FivetranGeneratorException(
                f"There is no Fivetran Connector for {source_db}.{fivetran_schema_db_naming}"
            )


class FivetranDbtGenerator(FivetranGenerator, BaseGenerator):
    def generate_tasks(
        self, params: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        params["connections_ids"] = self.get_pipeline_connection_list(params)
        return super().generate_tasks(params, config)
