from enum import Enum
from typing import List
from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ADDR
from raft_python.messages import HelloMessage, get_message_from_payload, ReqMessageType
from raft_python.kv_cache import KVCache


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

        # election node variables
        self.role: NodeRole = NodeRole.FOLLOWER
        self.num_votes_received: int = 0

    def send_hello(self):
        hello_msg: HelloMessage = HelloMessage(
            self.id, BROADCAST_ADDR, BROADCAST_ADDR)
        print("Replica %s starting up" % self.id, flush=True)
        self.socket.send(hello_msg)
        print("Sent hello message: %s" % hello_msg.serialize(), flush=True)

    def run(self):
        while True:
            msg = self.socket.receive()
            print("Received message '%s'" % (msg,), flush=True)
            req: ReqMessageType = get_message_from_payload(msg)
