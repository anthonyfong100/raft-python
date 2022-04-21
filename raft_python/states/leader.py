import time
import logging
import raft_python.messages as Messages
from raft_python.configs import LOGGER_NAME
from raft_python.states.state import State
logger = logging.getLogger(LOGGER_NAME)


class Leader(State):
    def __init__(self, old_state: "Candidate" = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        logging.info(f"Became the leader with Term:{self.term_number}")
        self.leader_id = self.raft_node.id
        self._register_loop_send_heartbeat()
        # self.match_index = {node: 0 for node in self.cluster_nodes}
        # self.next_index = {node: 0 for node in self.cluster_nodes}

    # TODO: Remove sending heartbeats
    def destroy(self):
        return

    def _register_loop_send_heartbeat(self):
        """Regularly send out heartbeat messages"""
        pass

    # TODO: send output
    def on_client_put(self, msg: Messages.PutMessageRequest):
        logger.debug(f"Received put request: {msg.serialize()}")
        pass

    # TODO: send redirect message
    def on_client_get(self, msg: Messages.GetMessageRequest):
        pass

    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        pass

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass
