#from airflow.models.dag import DAG
import dagfactory

config_file = "/app/examples/example_basic.yml"
example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
