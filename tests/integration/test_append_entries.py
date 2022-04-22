import unittest
from unittest.mock import Mock
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower

"""
Integration test for append entries. Aims to verify the following properties
1. If two entries in different logs have the same index
and term, then they store the same command.
2. If two entries in different logs have the same index
and term, then the logs are identical in all preceding
entries.
"""


class TestFollowerIntegration(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
