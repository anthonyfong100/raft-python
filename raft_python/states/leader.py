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
        self.leader_id = self.id
        self._register_loop_send_heartbeat()
        # self.match_index = {node: 0 for node in self.cluster_nodes}
        # self.next_index = {node: 0 for node in self.cluster_nodes}

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
        """Decide if we approve the new leader request"""
        is_valid_msg_term_number: bool = msg.term_number >= self.term_number
        is_valid_candidate: bool = self.voted_for is None or self.voted_for == msg.candidate_id
        is_valid_last_log_term_number: bool = (msg.last_log_term_number > self.log[-1].term_number) or \
            (msg.last_log_term_number >
             self.log[-1].term_number and msg.last_log_index >= len(self.log))

        is_vote_success = is_valid_msg_term_number and is_valid_candidate and is_valid_last_log_term_number
        if is_vote_success:
            self.voted_for = msg.candidate_id
            self.last_hearbeat = time.time()  # reset the heartbeat
            logger.info(
                f"Voting for {msg.candidate_id}")

        vote_response: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            self.term_number, is_vote_success)
        self.raft_node.send(vote_response)

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass
