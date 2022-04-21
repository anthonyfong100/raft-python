from enum import Enum
from typing import Union


class CommandType(Enum):
    SET = "set"
    GET = "get"


class BaseCommand:
    def __init__(self, term_number: int, command_type: CommandType, args: dict):
        self.term_number: int = term_number
        self.command_type: CommandType = command_type
        self.args = args

    def serialize(self) -> dict:
        serialized_dict: dict = vars(self).copy()
        serialized_dict["command_type"] = self.command_type.value
        return serialized_dict

    @staticmethod
    def deserialize(serialized_entry: dict):
        pass

    def __eq__(self, __o: object) -> bool:
        return self.serialize() == __o.serialize()


class SetCommand(BaseCommand):
    def __init__(self, term_number: int, args: dict):
        super().__init__(term_number, CommandType.SET, args)

    def is_valid(self):
        return "key" in self.args and "value" in self.args

    @staticmethod
    def deserialize(serialized_entry: dict):
        return SetCommand(serialized_entry["term_number"], serialized_entry["args"])


class GetCommand(BaseCommand):
    def __init__(self, term_number: int, args: dict):
        super().__init__(term_number, CommandType.GET, args)

    def is_valid(self):
        return "key" in self.args

    @staticmethod
    def deserialize(serialized_entry: dict):
        return GetCommand(serialized_entry["term_number"], serialized_entry["args"])


ALL_COMMANDS = Union[SetCommand, GetCommand]


def deserialize_command(serialized_entry: dict) -> ALL_COMMANDS:
    if serialized_entry["command_type"] == CommandType.GET.value:
        return GetCommand.deserialize(serialized_entry)
    elif serialized_entry["command_type"] == CommandType.SET.value:
        return SetCommand.deserialize(serialized_entry)
    raise ValueError(
        f"Cannot deserialize a command of unknown type. serialized entry:{serialized_entry}")
