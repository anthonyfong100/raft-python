import unittest
from raft_python.messages import HelloMessage
from raft_python.raft_node import RaftNode
from raft_python.socket_wrapper import SocketWrapper
from raft_python.kv_cache import KVCache
from raft_python.configs import BROADCAST_ALL_ADDR
from unittest.mock import Mock


class TestRaftNode(unittest.TestCase):
    def setUp(self):
        self.socket_mock = Mock(SocketWrapper)
        self.state_machine_mock = Mock(KVCache)
        self.raft_node = RaftNode(
            self.socket_mock, self.state_machine_mock, 0, 0)

    def test_send_hello(self):
        self.raft_node.send_hello()
        socket_send_args = HelloMessage(
            self.raft_node.id, BROADCAST_ALL_ADDR, BROADCAST_ALL_ADDR).serialize()
        self.socket_mock.send.assert_called_with(socket_send_args)


if __name__ == '__main__':
    unittest.main()
