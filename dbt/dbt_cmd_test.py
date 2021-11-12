from typing import Dict
import ipdb
import requests
import json
from requests.structures import CaseInsensitiveDict
from dbt_coves.core.exceptions import MissingDbtProject
import subprocess, sys

AIRBYTE_API_URL = "http://localhost:8001/api/v1/"
ENDPOINT_GET_CONN_BY_ID = AIRBYTE_API_URL + "web_backend/connections/get"
DBT_LIST_SOURCES_CMD = "dbt ls --resource-type source"
# AIRBYTE_API_ENDPOINTS: Dict(str, Dict(str, str)) = {
#     'get_connection_by_id' : {
#             'url':'connections/get',
#             'body': "{'connectionId':'ae731fab-7df7-4e35-8567-1beb81ef7dc4'}"            
#         }
# }
process = subprocess.run(DBT_LIST_SOURCES_CMD.split(), cwd="/home/bruno/dev/convexa/dag-factory/dbt", capture_output=True)
stdout = process.__getattribute__("stdout")
print(sys.getsizeof(stdout))



# r = requests.post(ENDPOINT_GET_CONN_BY_ID, data=json.dumps(r_body), headers=headers)
# print(r.text)