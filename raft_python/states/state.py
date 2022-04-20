import logging
import raft_python.messages as Messages
import time
from abc import ABC, abstractmethod, ABCMeta
from typing import Callable, List
from raft_python.commands import ALL_COMMANDS
from raft_python.configs import BROADCAST_ALL_ADDR, LOGGER_NAME
logger = logging.getLogger(LOGGER_NAME)


# Abstract class for all state methods
class State(ABC):
    __metaclass__ = ABCMeta

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
            self.cluster_nodes: List[str] = self.old_state
        else:
            self.raft_node: "RaftNode" = raft_node
            self.voted_for = None
            self.term_number = 0
            self.leader_id = BROADCAST_ALL_ADDR
            self.log = []
            self.cluster_nodes: List[str] = self.raft_node.others

        self.loop_send_messages = []
        self.last_hearbeat = time.time()

    def _get_method_from_msg(self, msg: Messages.InternalMessageType) -> Callable:
        method_mapping: dict = {
            Messages.GetMessageRequest: self.on_client_get,
            Messages.PutMessageRequest: self.on_client_put,
            Messages.RequestVote: self.on_interal_recv_request_vote,
            Messages.RequestVoteResponse: self.on_interal_recv_request_vote_response
        }
        call_method: Callable = method_mapping.get(msg.type, None)
        if call_method:
            return call_method(msg)
        logger.warning(
            f"Received invalid message with no method found\n message:{msg.serialize()} method mapping:{self.method_mapping}")
        return None

    def receive_internal_message(self, msg: Messages.InternalMessageType) -> None:
        """Receive internal message used for elections / voting and call the right method"""
        from raft_python.states.follower import Follower
        logger.debug('Received %s from %s', msg.serialize())

        if self.term_number < msg.term_number:
            # update term number to be the maximum
            self.term_number = msg.term_number

            if type(self) is not Follower:
                # set oneself to be follower
                self.raft_node.change_state(Follower)
                self.raft_node.state.receive_internal_message(msg)
                return

        call_method: Callable = self._get_method_from_msg(msg)
        if call_method:
            return call_method(msg)

    def received_client_message(self, msg: Messages.ClientMessageType):
        """Receive client messages from raft_node and call the right method"""
        call_method: Callable = self._get_method_from_msg(msg)
        if call_method:
            return call_method(msg)

    @abstractmethod
    def on_client_put(self, msg: Messages.PutMessageRequest):
        pass

    @abstractmethod
    def on_client_get(self, protocol, msg: Messages.GetMessageRequest):
        pass

    @abstractmethod
    def on_interal_recv_request_vote(self, msg: Messages.RequestVote):
        pass

    @abstractmethod
    def on_interal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass
