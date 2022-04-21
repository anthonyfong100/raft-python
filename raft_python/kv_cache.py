from typing import Any
from raft_python.commands import ALL_COMMANDS, CommandType

# TODO: Hide KV Cache behind state machine interface for future extensibility


class KVCache:
    def __init__(self):
        self.cache = {}

    def execute(self, command: ALL_COMMANDS) -> Any:
        if not command.is_valid:
            return
        if command.command_type == CommandType.SET:
            key, val = command.args.get("key"), command.args.get("value")
            self.cache[key] = val
        elif command.command_type == CommandType.GET:
            return self.cache.get(command.args.get("key"))
