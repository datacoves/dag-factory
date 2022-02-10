import glob
from airflow.models import DagBag
from socket import gaierror

dagbag = DagBag(include_examples=False)

import ipdb  # TODO ipdb import

ipdb.set_trace()  # TODO remove trace()

# for dag_file in glob.glob(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")):
for dag_file in glob.glob("/root/airflow/dags/*.py"):
    try:
        dagbag.process_file(dag_file)

    except gaierror:
        import ipdb  # TODO ipdb import

        ipdb.set_trace()  # TODO remove trace()
        pass

if dagbag.import_errors:
    print("The following DAGs present import errors:")
    for error in dagbag.import_errors:
        print(error)
    print("Review the stack above for details")


assert len(dagbag.import_errors) == 0
