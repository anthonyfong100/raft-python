import unittest
from raft_python.commands import SetCommand, GetCommand, deserialize_command


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

    def test_serialize_command(self):
        valid_set_command: SetCommand = SetCommand(1, {"key": 1, "value": 2})
        expected_serialization: dict = {
            'term_number': 1, 'command_type': 'set', 'args': {'key': 1, 'value': 2}}
        self.assertDictEqual(valid_set_command.serialize(),
                             expected_serialization)

        valid_get_command: GetCommand = GetCommand(1, {"key": 1})
        expected_serialization: dict = {
            'term_number': 1, 'command_type': 'get', 'args': {'key': 1}}
        self.assertDictEqual(valid_get_command.serialize(),
                             expected_serialization)

    def test_deserialize_command(self):
        set_command: SetCommand = SetCommand(
            1, {"key": 1, "value": 2})
        self.assertEqual(set_command.deserialize(set_command.serialize()),
                         set_command)

        get_command: GetCommand = GetCommand(
            1, {"key": 1})
        self.assertEqual(get_command.deserialize(get_command.serialize()),
                         get_command)

    def test_deserialize_command_function(self):
        set_command: SetCommand = SetCommand(1, {"key": 1, "value": 2})
        set_command_serialized: dict = {
            'term_number': 1, 'command_type': 'set', 'args': {'key': 1, 'value': 2}}
        self.assertEqual(deserialize_command(
            set_command_serialized), set_command)

        get_command: SetCommand = GetCommand(1, {"key": 1})
        get_command_serialized: dict = {
            'term_number': 1, 'command_type': 'get', 'args': {'key': 1}}
        self.assertEqual(deserialize_command(
            get_command_serialized), get_command)


if __name__ == '__main__':
    unittest.main()
