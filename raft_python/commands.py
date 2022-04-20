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


class SetCommand(BaseCommand):
    def __init__(self, term_number: int, args: dict):
        super().__init__(term_number, CommandType.SET, args)

    def is_valid(self):
        return "key" in self.args and "value" in self.args


class GetCommand(BaseCommand):
    def __init__(self, term_number: int, args: dict):
        super().__init__(term_number, CommandType.GET, args)

    def is_valid(self):
        return "key" in self.args


ALL_COMMANDS = Union[SetCommand, GetCommand]
