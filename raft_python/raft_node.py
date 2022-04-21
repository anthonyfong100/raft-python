import select
import time
import logging
from enum import Enum
from typing import List
# from raft_python.states import ALL_NODE_STATES
from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT, LOGGER_NAME
from raft_python.messages import GetMessageRequest,\
    HelloMessage, MessageFail, PutMessageRequest, RequestVoteResponse,\
    get_message_from_payload, IncomingMessageType, RequestVote
from raft_python.kv_cache import KVCache
from raft_python.commands import ALL_COMMANDS

logger = logging.getLogger(LOGGER_NAME)


class NodeRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, socket_wrapper: SocketWrapper, kv_cache: KVCache, id: str, others: List[str]):
        self.socket: SocketWrapper = socket_wrapper
        # TODO: refactor this to interact with executor interface
        self.executor: KVCache = kv_cache
        self.id: str = id
        self.others: List[str] = others
        self.state = None  # need to register state here

        logger.info("Starting Raft Node")

    def send_hello(self):
        hello_msg: HelloMessage = HelloMessage(
            self.id, BROADCAST_ALL_ADDR, BROADCAST_ALL_ADDR)
        logger.info("Replica %s starting up" % self.id)
        self.socket.send(hello_msg)

    def send(self, message: IncomingMessageType):
        """Wrapper to call internal socket Manager to send message"""
        self.socket.send(message)

    # State wrapper functions
    def register_state(self, state: "ALL_NODE_STATES"):
        self.state = state

    def change_state(self, new_state: "ALL_NODE_STATES"):
        logger.info(
            f"State changed from {type(self.state)} to {type(new_state)}")
        new_created_state: "ALL_NODE_STATES" = new_state(self.state, self)
        self.state = new_created_state

    # TODO: Wire up all the methods calls to state & add heartbeat mechanism
    def run(self):
        while True:
            # check if there are any looping messages
            if self.state.node_raft_command is not None:
                # execute any commands
                if time.time() > self.state.execution_time:
                    # execute here
                    if self.state.args is not None:
                        self.state.node_raft_command(self.state.args)
                    else:
                        print(self.state.node_raft_command,
                              self.state.args, type(self.state))
                        self.state.node_raft_command()

            # make socket connection non-blocking
            socket = select.select([self.socket.socket], [], [], 0.1)[0]
            for _ in socket:
                msg = self.socket.receive()
                req: IncomingMessageType = get_message_from_payload(msg)
                self.state.receive_message(req)
