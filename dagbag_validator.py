import unittest, os
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag(dag_folder=os.environ.get("AIRFLOW__CORE__DAGS_FOLDER"))
        print(f"Validating the following DAGs:")
        print(*self.dagbag.dags)

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            "DAG import failures. Errors: {}".format(self.dagbag.import_errors),
        )


test_dag = TestDagIntegrity()
suite = unittest.TestLoader().loadTestsFromTestCase(test_dag)
unittest.TextTestRunner(verbosity=2).run(suite)
