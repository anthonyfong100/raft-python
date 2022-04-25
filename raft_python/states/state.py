from email.message import Message
import logging
import raft_python.messages as Messages
import time
from abc import ABC, abstractmethod
from typing import Callable, List
from raft_python.commands import ALL_COMMANDS
from raft_python.configs import BROADCAST_ALL_ADDR, LOGGER_NAME
logger = logging.getLogger(LOGGER_NAME)


# Abstract class for all state methods
class State(ABC):
    def __init__(self, old_state: "State" = None, raft_node: "RaftNode" = None):
        """State is initialized passing an orchestator instance when first
        deployed. Subsequent state changes use the old_state parameter to
        preserve the environment.
        """
        if old_state:
            self.raft_node: "RaftNode" = old_state.raft_node
            self.voted_for: str = old_state.voted_for
            self.term_number: int = old_state.term_number
            self.leader_id: str = old_state.leader_id
            self.log: List[ALL_COMMANDS] = old_state.log.copy()
            self.cluster_nodes: List[str] = old_state.cluster_nodes
            self.commit_index = old_state.commit_index
        else:
            self.raft_node: "RaftNode" = raft_node
            self.voted_for = None
            self.term_number = 0
            self.leader_id = BROADCAST_ALL_ADDR
            self.log = []
            self.cluster_nodes: List[str] = self.raft_node.others + \
                [self.raft_node.id]
            # commit index represents the index of last log entry commited, start with -1
            self.commit_index = -1

        self.last_hearbeat = time.time()
        self.node_raft_command = None
        self.execution_time = None
        self.args = None

    def destroy(self):
        pass

    def __str__(self) -> str:
        dict = vars(self).copy()
        output = ""
        for key, value in dict.items():
            output += f"{key}:{value}\n"
        return output

    def _get_method_from_msg(self, msg: Messages.InternalMessageType) -> Callable:
        method_mapping: dict = {
            Messages.GetMessageRequest: self.on_client_get,
            Messages.PutMessageRequest: self.on_client_put,
            Messages.RequestVote: self.on_internal_recv_request_vote,
            Messages.RequestVoteResponse: self.on_internal_recv_request_vote_response,
            Messages.AppendEntriesReq: self.on_internal_recv_append_entries,
            Messages.AppendEntriesResponse: self.on_internal_recv_append_entries_response}
        call_method: Callable = method_mapping.get(type(msg), None)
        if call_method:
            return call_method(msg)
        logger.warning(
            f"Received invalid message with no method found\n message:{type(msg)} method mapping:{method_mapping}")
        return None

    def receive_internal_message(self, msg: Messages.InternalMessageType) -> None:
        """Receive internal message used for elections / voting and call the right method"""
        call_method: Callable = self._get_method_from_msg(msg)
        if call_method:
            return call_method(msg)

    def received_client_message(self, msg: Messages.ClientMessageType):
        """Receive client messages from raft_node and call the right method"""
        call_method: Callable = self._get_method_from_msg(msg)
        if call_method:
            return call_method(msg)

    def receive_message(self, msg: Messages.IncomingMessageType):
        if Messages.is_client_message(msg):
            return self.received_client_message(msg)
        elif Messages.is_internal_message(msg):
            return self.receive_internal_message(msg)
        raise ValueError(f"Invalid messaged received of type:{type(msg)}")

    @abstractmethod
    def on_client_put(self, msg: Messages.PutMessageRequest):
        pass

    @abstractmethod
    def on_client_get(self, protocol, msg: Messages.GetMessageRequest):
        pass

    @abstractmethod
    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        pass

    @abstractmethod
    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass

    @abstractmethod
    def on_internal_recv_append_entries(self, msg: Messages.AppendEntriesReq):
        pass

    @abstractmethod
    def on_internal_recv_append_entries_response(self, msg: Messages.AppendEntriesResponse):
        pass
