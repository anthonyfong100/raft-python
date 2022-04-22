import unittest
from raft_python.commands import SetCommand, GetCommand, CommandType
from raft_python.kv_cache import KVCache


class TestKVCache(unittest.TestCase):
    def setUp(self) -> None:
        self.kv_cache = KVCache()

    def test_execute_set(self):
        command: SetCommand = SetCommand(
            0, {"key": "name", "value": "anthony"})
        self.kv_cache.execute(command)
        self.assertEqual(
            self.kv_cache.cache["name"], "anthony", "Cache should set name to be anthony")

        command: SetCommand = SetCommand(
            0, {"key": "name", "value": "alfred"})
        self.kv_cache.execute(command)
        self.assertEqual(
            self.kv_cache.cache["name"], "alfred", "Cache should reset name to be alfred")

    def test_execute_get(self):
        command: SetCommand = SetCommand(
            0, {"key": "name", "value": "anthony"})
        self.kv_cache.execute(command)
        self.assertEqual(
            self.kv_cache.cache["name"], "anthony", "Cache should set name to be anthony")

        command: SetCommand = GetCommand(
            0, {"key": "name"})
        resp = self.kv_cache.execute(command)
        self.assertEqual(
            resp, "anthony", "Cache should return anthony")


if __name__ == '__main__':
    unittest.main()
