from airflow import DAG
import dagfactory

config_file = "/app/examples/example_basic.yml"
example_dag_factory = dagfactory.DagFactory(config_file)
# import ipdb

# ipdb.set_trace()
example_dag_factory.get_dag_configs()


# Creating task dependencies
example_dag_factory.clean_dags(globals())


example_dag_factory.generate_dags(globals())
