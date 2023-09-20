import unittest

from prefect.utilities.storage import extract_flow_from_file


class TestHelloWorld(unittest.TestCase):
    """
    Class to test Hello world flow
    """

    def setUp(self) -> None:
        self.flow = extract_flow_from_file('flows/test_project/hello_world/flow.py')

    def test_flow_run(self):
        state = self.flow.run()
        self.assertTrue(state.is_successful())
