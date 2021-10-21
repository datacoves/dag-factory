class AirbyteDbtGenerator:
    def __init__(self, dag_builder):
        self.dag_builder = dag_builder

    def generate_tasks(self, params):
        """
        Connects to airbyte server, gets sources and creates a new task for each source
        """
        # call self.dag_builder to generate new tasks
        pass
