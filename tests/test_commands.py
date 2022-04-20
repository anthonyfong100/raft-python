import unittest
from raft_python.commands import SetCommand, GetCommand, CommandType


class TestMessageTypes(unittest.TestCase):
    def test_command_is_valid(self):
        valid_set_command: SetCommand = SetCommand(1, {"key": 1, "value": 2})
        self.assertTrue(valid_set_command.is_valid(),
                        "Set command should have key and value")

        invalid_set_command: SetCommand = SetCommand(1, {"value": 2})
        self.assertFalse(invalid_set_command.is_valid(),
                         "Set command should have key and value")

        valid_get_command: GetCommand = GetCommand(1, {"key": 1})
        self.assertTrue(valid_get_command.is_valid(),
                        "Get command should have key")

        invalid_get_command: GetCommand = GetCommand(1, {"value": 2})
        self.assertFalse(invalid_get_command.is_valid(),
                         "Get command should have key")


if __name__ == '__main__':
    unittest.main()
