import unittest
from raft_python.kv_store import KVStore
from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ADDR
from unittest.mock import Mock


class TestKVStore(unittest.TestCase):
    def setUp(self):
        self.socket_mock = Mock(SocketWrapper)
        self.kv_store = KVStore(self.socket_mock, 0, 0)

    def test_intiialization(self):
        self.socket_mock.send.return_value = {"src": self.kv_store.id, "dst": BROADCAST_ADDR,
                                              "leader": BROADCAST_ADDR, "type": "hello"}
        self.socket_mock.send.assert_called()


if __name__ == '__main__':
    unittest.main()
