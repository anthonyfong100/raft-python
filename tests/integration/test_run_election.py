import unittest
from raft_python.states.follower import Follower
from raft_python.states.candidate import Candidate
from raft_python.states.leader import Leader
from tests.integration.simulator import Simulator
"""
Integration test for running elections. Aims to verify the following properties
1. One one leader at a given point of time
2. In the case of no network failures --> only one leader per session
3. Leader election process is retriggered --> from timeout
"""


class TestRunElection(unittest.TestCase):
    def setUp(self):
        # simulate with 5 Nodes
        self.simulator = Simulator(5)

    def tearDown(self) -> None:
        self.simulator.teardown()

    def test_run_election_one_leader_5_nodes(self):
        """
        Test that there should only be one leader during the election
        """
        self.simulator.initialize()
        self.simulator.run(wait_timeout=1, run_timeout=2)
        num_leaders = len(list(filter(lambda raft_node: type(
            raft_node.state) == Leader, self.simulator.raft_node_mapping.values())))
        num_followers = len(list(filter(lambda raft_node: type(
            raft_node.state) == Follower, self.simulator.raft_node_mapping.values())))
        num_candidatess = len(list(filter(lambda raft_node: type(
            raft_node.state) == Candidate, self.simulator.raft_node_mapping.values())))
        self.assertEqual(num_followers, 4)
        self.assertEqual(num_candidatess, 0)
        self.assertEqual(num_leaders, 1)

    def test_run_election_one_leader_2_nodes(self):
        """
        Test that there should only be one leader during the election
        """
        simulator = Simulator(2)
        simulator.initialize()
        simulator.run(wait_timeout=1, run_timeout=2)
        simulator.teardown()
        num_leaders = len(list(filter(lambda raft_node: type(
            raft_node.state) == Leader, simulator.raft_node_mapping.values())))
        num_followers = len(list(filter(lambda raft_node: type(
            raft_node.state) == Follower, simulator.raft_node_mapping.values())))
        num_candidatess = len(list(filter(lambda raft_node: type(
            raft_node.state) == Candidate, simulator.raft_node_mapping.values())))
        self.assertEqual(num_followers, 1)
        self.assertEqual(num_candidatess, 0)
        self.assertEqual(num_leaders, 1)

    def test_run_election_one_leader_1_nodes(self):
        """
        Test that there should only be one leader during the election
        """
        simulator = Simulator(1)
        simulator.initialize()
        simulator.run(wait_timeout=1, run_timeout=2)
        simulator.teardown()
        num_leaders = len(list(filter(lambda raft_node: type(
            raft_node.state) == Leader, simulator.raft_node_mapping.values())))
        num_followers = len(list(filter(lambda raft_node: type(
            raft_node.state) == Follower, simulator.raft_node_mapping.values())))
        num_candidatess = len(list(filter(lambda raft_node: type(
            raft_node.state) == Candidate, simulator.raft_node_mapping.values())))
        self.assertEqual(num_followers, 0)
        self.assertEqual(num_candidatess, 0)
        self.assertEqual(num_leaders, 1)


if __name__ == '__main__':
    unittest.main()
