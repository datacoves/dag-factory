from airflow import DAG

import dagfactory

config_file = "/root/airflow/dags/new_ms_teams_ymldag.yml"
example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
