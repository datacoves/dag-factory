import json
import shlex
import subprocess
from os import environ
from pathlib import Path
from typing import Any, Dict

import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from requests.exceptions import RequestException
from slugify import slugify

from .base import BaseGenerator


class FivetranGeneratorException(Exception):
    pass


class FivetranGenerator(BaseGenerator):
    def get_pipeline_connection(params, database, schema, table):
        pass

    def generate_tasks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["connections_ids"] = self.get_pipeline_connection_list()
        tasks: Dict[str, BaseOperator] = dict()

        pass

    def get_pipeline_connection(
        source_db: str, source_schema: str, source_table: str
    ) -> Dict[Any:Any]:
        """
        Use dbt source's database, schema and table to find Fivetran connectionId
        """
        pass
