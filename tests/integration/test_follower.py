import unittest
from unittest.mock import Mock
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower

# Integration test for follower


class TestFollowerIntegration(unittest.TestCase):
    def setUp(self):
        self.raft_node_mock = Mock(RaftNode)
        self.raft_node_mock.id = 0
        self.raft_node_mock.others = ["1", "2", "3", "4"]
        self.follower_state = Follower(raft_node=self.raft_node_mock)
        self.raft_node_mock.state = self.follower_state

    def test_random_testing(self):
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
