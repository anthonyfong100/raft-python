import unittest
from raft_python.raft_node import RaftNode
from raft_python.socket_wrapper import SocketWrapper
from raft_python.kv_cache import KVCache
from raft_python.configs import BROADCAST_ADDR
from unittest.mock import Mock


class TestRaftNode(unittest.TestCase):
    def setUp(self):
        self.socket_mock = Mock(SocketWrapper)
        self.state_machine_mock = Mock(KVCache)
        self.raft_node = RaftNode(
            self.socket_mock, self.state_machine_mock, 0, 0)

    def test_send_hello(self):
        self.raft_node.send_hello()
        self.socket_mock.send.return_value = {"src": self.raft_node.id, "dst": BROADCAST_ADDR,
                                              "leader": BROADCAST_ADDR, "type": "hello"}
        self.socket_mock.send.assert_called()


if __name__ == '__main__':
    unittest.main()
