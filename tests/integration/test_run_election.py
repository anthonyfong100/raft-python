import unittest
from tests.integration.simulator import Simulator
"""
Integration test for running elections. Aims to verify the following properties
1. One one leader at a given point of time
2. In the case of no network failures --> only one leader per session
3. Leader election process is retriggered --> from timeout
"""


class TestFollowerIntegration(unittest.TestCase):
    def setUp(self):
        # simulate with 5 Nodes
        self.simulator = Simulator(5)

    def tearDown(self) -> None:
        return self.simulator.teardown()

    def test_random_testing(self):
        self.simulator.run(wait_timeout=5, run_timeout=3)


if __name__ == '__main__':
    unittest.main()
