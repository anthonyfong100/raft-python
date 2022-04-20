from enum import Enum
from typing import List
from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ALL_ADDR
from raft_python.messages import GetMessageRequest, HelloMessage, PutMessageRequest, RequestVoteResponse, get_message_from_payload, ReqMessageType, RequestVote
from raft_python.kv_cache import KVCache
from raft_python.commands import ALL_COMMANDS


class NodeRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, socket_wrapper: SocketWrapper, kv_cache: KVCache, id: str, others: List[str]):
        self.socket: SocketWrapper = socket_wrapper
        # TODO: refactor this to interact with state machine interface
        self.state_machine: KVCache = kv_cache
        self.id: str = id
        self.others: List[str] = others

        # raft node state variables
        self.term = 0
        self.log: List[ALL_COMMANDS] = []

        # election node variables
        self.role: NodeRole = NodeRole.FOLLOWER
        self.num_votes_received: int = 0

    def send_hello(self):
        hello_msg: HelloMessage = HelloMessage(
            self.id, BROADCAST_ALL_ADDR, BROADCAST_ALL_ADDR)
        print("Replica %s starting up" % self.id, flush=True)
        self.socket.send(hello_msg)
        print("Sent hello message: %s" % hello_msg.serialize(), flush=True)

    def run_election(self) -> None:
        if self.role != NodeRole.CANDIDATE:
            return
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
                self.id,
                BROADCAST_ALL_ADDR)  # not sure who is the leader so broadcast to all
            self.socket.send(request_vote)

    def process_get_client_req(self, req: GetMessageRequest) -> None:
        print("Received Client Req message '%s'" % (req,), flush=True)

    def process_request_vote_req(self, req: RequestVote) -> None:
        print("Received Election Req message '%s'" % (req,), flush=True)

    def process_request_vote_response(self, req: RequestVoteResponse) -> None:
        print("Received Election Response message '%s'" % (req,), flush=True)

    def process_response(self, req: ReqMessageType) -> any:
        if type(req) == GetMessageRequest or type(req) == PutMessageRequest:
            return self.process_get_client_req(req)
        elif type(req) == RequestVote:
            return self.process_request_vote_req(req)
        elif type(req) == RequestVoteResponse:
            return self.process_request_vote_response(req)
        raise ValueError(
            f"received unknown message type:{type(req)} message:{req.serialize()}")

    def run(self):
        while True:
            msg = self.socket.receive()
            req: ReqMessageType = get_message_from_payload(msg)
            self.process_response(req)
