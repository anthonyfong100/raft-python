from typing import Optional
from raft_python.commands import ALL_COMMANDS, CommandType

# TODO: Hide KV Cache behind state machine interface for future extensibility


class KVCache:
    def __init__(self):
        self.cache = {}

    def execute(self, command: ALL_COMMANDS) -> Optional[str]:
        if not command.is_valid:
            return
        if command.command_type == CommandType.SET:
            key, value = command.args.get("key"), command.args.get("value")
            self.cache[key] = value
        elif command.command_type == CommandType.GET:
            return self.cache.get(command.args.get("key"), None)
