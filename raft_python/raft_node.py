import select
import time
import logging
from enum import Enum
from typing import List
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

        # raft node state variables
        self.term = 0
        self.log: List[ALL_COMMANDS] = []

        # election node variables
        self.role: NodeRole = NodeRole.FOLLOWER
        self.num_votes_received: int = 0
        self.leader = BROADCAST_ALL_ADDR
        self.last_heartbeat = time.time()
        logger.info("Starting Raft Node")

    def send_hello(self):
        hello_msg: HelloMessage = HelloMessage(
            self.id, BROADCAST_ALL_ADDR, BROADCAST_ALL_ADDR)
        print("Replica %s starting up" % self.id, flush=True)
        self.socket.send(hello_msg)

    def run_election(self) -> None:
        if self.role != NodeRole.FOLLOWER:
            return
        logger.info("Running elections")
        self.role = NodeRole.CANDIDATE
        self.term += 1
        self.num_votes_received = 1
        for other_node_id in self.others:
            last_log_index: int = len(self.log)
            last_log_term_number: int = self.log[-1].term_number if self.log else 0
            request_vote: RequestVote = RequestVote(
                self.id,
                other_node_id,
                self.term,
                self.id,
                last_log_index,
                last_log_term_number,
                BROADCAST_ALL_ADDR)  # not sure who is the leader so broadcast to all
            self.socket.send(request_vote)

    #TODO: implement
    def process_get_client_req(self, req: GetMessageRequest) -> None:
        logger.debug("Received Client Req message '%s'" %
                     (req.serialize()))
        failed_resp: MessageFail = MessageFail(
            self.id, req.src, req.MID, self.leader)
        self.socket.send(failed_resp)

    def process_request_vote_req(self, req: RequestVote) -> None:
        logger.info("Received Election Req message '%s'" % (req,))

    def process_request_vote_response(self, req: RequestVoteResponse) -> None:
        logger.debug("Received Election Response message '%s'" %
                     (req,))

    def process_response(self, req: ReqMessageType) -> any:
        if type(req) == GetMessageRequest or type(req) == PutMessageRequest:
            return self.process_get_client_req(req)
        elif type(req) == RequestVote:
            return self.process_request_vote_req(req)
        elif type(req) == RequestVoteResponse:
            return self.process_request_vote_response(req)
        raise ValueError(
            f"received unknown message type:{type(req)} message:{req.serialize()}")

    def should_run_election(self):
        return time.time() > self.last_heartbeat + MAX_DURATION_NO_HEARTBEAT and self.role == NodeRole.FOLLOWER

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
