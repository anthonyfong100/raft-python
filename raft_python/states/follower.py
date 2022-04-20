import time
import logging
import random
import raft_python.messages as Messages
from raft_python.states.state import State
from raft_python.configs import MAX_DURATION_NO_HEARTBEAT, LOGGER_NAME
logger = logging.getLogger(LOGGER_NAME)


class Follower(State):
    def __init__(self, old_state: State = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        self.voted_for = None
        self.election_timer = self.randomly_generate_election_timer()

    def randomly_generate_election_timer(self, timer_range=MAX_DURATION_NO_HEARTBEAT):
        """Generate a random election timer within range"""
        election_timer_ms: int = random.randint(
            timer_range * 1000, 2 * 1000 * timer_range)
        return election_timer_ms / 1000.0

    def on_client_put(self, msg: Messages.PutMessageRequest):
        redirect_message: Messages.MessageRedirect = Messages.MessageRedirect(
            self.raft_node.id,
            msg.src,
            msg.MID,
            self.leader_id,
        )
        self.raft_node.send(redirect_message)

    def on_client_get(self, msg: Messages.GetMessageRequest):
        redirect_message: Messages.MessageRedirect = Messages.MessageRedirect(
            self.raft_node.id,
            msg.src,
            msg.MID,
            self.leader_id,
        )
        self.raft_node.send(redirect_message)

    def on_interal_recv_request_vote(self, msg: Messages.RequestVote):
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

    def on_interal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass
