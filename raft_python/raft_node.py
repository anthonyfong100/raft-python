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
    get_message_from_payload, ReqMessageType, RequestVote
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
        logger.info("Replica %s starting up" % self.id, flush=True)
        self.socket.send(hello_msg)

    def process_response(self, req: ReqMessageType) -> any:
        if type(req) == GetMessageRequest or type(req) == PutMessageRequest:
            return self.process_get_client_req(req)
        elif type(req) == RequestVote:
            return self.process_request_vote_req(req)
        elif type(req) == RequestVoteResponse:
            return self.process_request_vote_response(req)
        raise ValueError(
            f"received unknown message type:{type(req)} message:{req.serialize()}")

    def send(self, message: ReqMessageType):
        """Wrapper to call internal socket Manager to send message"""
        self.socket.send(message)

    # State wrapper functions
    def register_state(self, state: "ALL_NODE_STATES"):
        self.state = state

    def change_state(self, new_state: "ALL_NODE_STATES"):
        logging.info(
            f"State changed from {type(self.state)} to {type(new_state)}")
        new_created_state: "ALL_NODE_STATES" = new_state(self.state, self)
        self.state = new_created_state

    # TODO: Wire up all the methods calls to state & add heartbeat mechanism
    def run(self):
        while True:
            if self.should_run_election():
                self.run_election()

            # make socket connection non-blocking
            socket = select.select([self.socket.socket], [], [], 0.1)[0]
            for _ in socket:
                msg = self.socket.receive()
                req: ReqMessageType = get_message_from_payload(msg)
                self.process_response(req)
