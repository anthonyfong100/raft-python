from typing import Any
from raft_python.commands import Command, CommandType


class KVCache:
    def __init__(self):
        self.cache = {}

    def execute(self, command: Command) -> Any:
        if not command.is_valid:
            return
        if command.command_type == CommandType.SET:
            key, val = command.args.key, command.args.val
            self.cache[key] = val
        elif command.command_type == CommandType.GET:
            return self.cache.get(command.args.key)
