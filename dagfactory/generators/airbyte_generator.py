from typing import Any,Dict, Generator, Type
from airflow.models import BaseOperator
from airflow.models.base import Base
from requests.exceptions import RequestException
from ..dagfactory import DagBuilder
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.hooks.base import BaseHook
import subprocess, requests, json

# IMPORTS FOR TESTING
import ipdb


class AirbyteGeneratorException(Exception):
    pass

class AirbyteGenerator:
    def __init__(self, dag_builder, params): 
        self.dag_builder = dag_builder
        self.AIRFLOW_OPERATOR_FULL_PATH = AirbyteTriggerSyncOperator.__module__+'.'+AirbyteTriggerSyncOperator.__qualname__
        self.AIRBYTE_TASK_ID_PREFIX = "AIRBYTE_SYNC_TASK_"
        self.AIRBYTE_DESTINATION_TABLE_PREFIX = "_AIRBYTE_RAW_"
        self.DBT_CMD_LIST_SOURCES = "dbt ls --resource-type source"

        try:
            airbyte_connection_name = params["airflow_connection_id"]
            airbyte_connection = BaseHook.get_connection(airbyte_connection_name)
        except KeyError:
            raise AirbyteGeneratorException("`airflow_connection_id` is missing in DAG's configuration YAML file")

        AIRBYTE_API_URL = f"http://{airbyte_connection.host}:{airbyte_connection.port}/api/v1/"
        AIRBYTE_API_ENDPOINT_LIST_ENTITY = AIRBYTE_API_URL + "{entity}/list"
        AIRBYTE_API_ENDPOINT_LIST_WORKSPACES = AIRBYTE_API_ENDPOINT_LIST_ENTITY.format(entity="workspaces")
        AIRBYTE_API_ENDPOINT_LIST_CONNECTIONS = AIRBYTE_API_ENDPOINT_LIST_ENTITY.format(entity="connections")
        AIRBYTE_API_ENDPOINT_LIST_DESTINATIONS = AIRBYTE_API_ENDPOINT_LIST_ENTITY.format(entity="destinations")

        self.airbyte_workspace_id = self.airbyte_api_call(AIRBYTE_API_ENDPOINT_LIST_WORKSPACES, "workspaces")["workspaceId"]
        self.airbyte_api_standard_req_body = {"workspaceId": self.airbyte_workspace_id}
        self.airbyte_connections = self.airbyte_api_call(AIRBYTE_API_ENDPOINT_LIST_CONNECTIONS, "connections", self.airbyte_api_standard_req_body)
        self.airbyte_destinations = self.airbyte_api_call(AIRBYTE_API_ENDPOINT_LIST_DESTINATIONS, "destinations", self.airbyte_api_standard_req_body)
        

    def airbyte_api_call(self, endpoint: str, call_keyword: str, request_body: Dict[str, str] = None):
        """
        Generic `api caller` for contacting Airbyte
        """
        try:
            if request_body is not None:
                response = requests.post(endpoint, json=request_body)
            else:
                response = requests.post(endpoint)
            
            if response.status_code == 200:
                response_json = json.loads(response.text)[call_keyword]
                if len(response_json) <= 1:
                    return response_json[0] or False
                else:
                    return response_json
            else:
                raise RequestException 
        except requests.exceptions.RequestException as e:
            raise AirbyteGeneratorException("Airbyte API error: " + e)

    def generate_sync_task(self, params: Dict[str, Any], operator:str) -> Dict[str, Any]:
        """
        Standard Airflow call to `make_task`
            same logic as dagbuilder.py: 
                this re-usage is why params(dict) must be cleaned up from any extra information that the corresponding operator can't handle
        """
        return self.dag_builder.make_task(
            operator=operator,
            task_params=params,
        )

    def remove_inexistant_conn_ids(self, connections_ids):
        """
        Overwrite the user-written `connections_ids` list (.yml) with the ones actually existing inside Airbyte\n
        If not handled, a lot of Airflow tasks would fail.
        """
        return [c_id for c_id in connections_ids if c_id in [connection['connectionId'] for connection in self.airbyte_connections]]       

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        AirbyteGenerator entry-point from both `dagbuilder` and `AirbyteDbtGenerator`\n
            Clean params dictionary from information that Airflow can't handle\n
            Fill and return the tasks dictionary        
        """
        params["airbyte_conn_id"] = params.pop("airflow_connection_id")        
        connections_ids = params.pop("connections_ids")
        
        generator_class = params.pop("generator")
        if 'AirbyteGenerator' in generator_class:
            connections_ids = self.remove_inexistant_conn_ids(connections_ids)        

        tasks: Dict[str, BaseOperator] = {}
        try:            
            for conn_id in connections_ids:
                task_id = self.AIRBYTE_TASK_ID_PREFIX + str(conn_id)
                params["task_id"] = task_id
                params["connection_id"] = conn_id                
                tasks[task_id] = self.generate_sync_task(params, self.AIRFLOW_OPERATOR_FULL_PATH)
        except Exception as e:
            raise e

        return tasks

class AirbyteDbtGenerator(AirbyteGenerator):
    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:  
        connections_ids = []
        
        dbt_project_path = params.pop('dbt_project_path')        
        dbt_selector = params.pop('dbt_selector', None)        

        dbt_list_sources_cmd = self.DBT_CMD_LIST_SOURCES.split()
        if dbt_selector:
            dbt_list_sources_cmd += dbt_selector

        # Call DBT on the specified path
        process = subprocess.run(dbt_list_sources_cmd, cwd=dbt_project_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stderr = process.stderr.decode()
        stdout = process.stdout.decode()

        try:
            if not stderr and stdout:
                sources_list = [src.lstrip("source:") for src in stdout.split("\n") if src]

                for conn in self.airbyte_connections:
                    connection_id = conn['connectionId']
                    for stream in conn["syncCatalog"]["streams"]:
                        airbyte_candidate_name = (self.AIRBYTE_DESTINATION_TABLE_PREFIX + stream["stream"]["name"]).lower()

                        for source in sources_list:
                            # Transform the 'dbt source' into [db, schema, table]
                            source_db, source_schema, source_table = [element.lower() for element in source.split('.')]

                            if source_table == airbyte_candidate_name:                                                       
                                destination_id = conn['destinationId']
                                
                                for destination in self.airbyte_destinations:
                                    if destination['destinationId'] == destination_id:                                
                                        connection_config = destination['connectionConfiguration']                            

                                try:
                                    if (source_db == connection_config['database'].lower()) and (source_schema == connection_config['schema'].lower()):
                                            connections_ids.append(connection_id)
                                except TypeError:
                                    raise AirbyteGeneratorException(f"Airbyte error: there are no destinations for conn")
                                
            
            params['connections_ids'] = connections_ids
        except Exception as e:
            raise AirbyteGeneratorException(e)

        return super().generate_tasks(params)
            

            



