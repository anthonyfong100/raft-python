import time
import logging
import random
import raft_python.messages as Messages
from raft_python.states.state import State
from raft_python.configs import MAX_DURATION_NO_HEARTBEAT, LOGGER_NAME
from raft_python.commands import deserialize_command
logger = logging.getLogger(LOGGER_NAME)


class Follower(State):
    name = "Follower"

    def __init__(self, old_state: State = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        self.voted_for = None
        # TODO: fix prevent cyclic import dependency
        from raft_python.states.candidate import Candidate
        self.node_raft_command = self.raft_node.change_state
        self.args = (Candidate)

        self._reset_timeout()

    # TODO: Check if the election timer should be always randomized or only once in the beginning
    def _reset_timeout(self):
        """ 
        Resets the last heartbeat, randomize the number election timer and generate the next execution time for voting
        """
        self.last_hearbeat = time.time()
        self.election_timer = self.randomly_generate_election_timer()
        self.execution_time = self.last_hearbeat + self.election_timer

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

    def _should_accept_vote(self, msg: Messages.RequestVote) -> bool:
        is_valid_msg_term_number: bool = msg.term_number >= self.term_number
        is_valid_candidate: bool = self.voted_for is None or self.voted_for == msg.candidate_id
        last_log_term_number_follower: int = self.log[-1].term_number if self.log else 0
        is_valid_last_log_term_number: bool = (msg.last_log_term_number > last_log_term_number_follower) or \
            (msg.last_log_term_number == last_log_term_number_follower and
             msg.last_log_index >= len(self.log))
        return is_valid_msg_term_number and is_valid_candidate and is_valid_last_log_term_number

    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        """Decide if we approve the new leader request"""
        should_accept_vote: bool = self._should_accept_vote(msg)
        if should_accept_vote:
            self.voted_for = msg.candidate_id
            self._reset_timeout()
            # https://courses.grainger.illinois.edu/ece428/sp2020//assets/slides/lect17.pdf
            self.term_number = msg.term_number
            logger.info(
                f"Voting for {msg.candidate_id}")

        vote_response: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            self.raft_node.id, msg.src,
            self.term_number,
            should_accept_vote,
            self.leader_id
        )
        self.raft_node.send(vote_response)

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass

    def _is_valid_append_entries_req(self, msg: Messages.AppendEntriesReq):
        """
        Check if term number in message > current term number
        Check if term number at prev log index == msg prev log term number
        """
        term_number_is_valid: bool = msg.term_number >= msg.term_number
        if len(self.log) > msg.prev_log_index and self.log[msg.prev_log_index].term_number != msg.prev_log_term_number:
            return False
        return term_number_is_valid

    # TODO check if need to update term number here
    def on_internal_recv_append_entries(self, msg: Messages.AppendEntriesReq):
        is_success: bool = self._is_valid_append_entries_req(msg)
        if msg.term_number >= self.term_number:
            # reset election counter
            self._reset_timeout()
        if is_success:
            # remove log terms from index onwards
            self.log = self.log[:msg.prev_log_index]
            self.leader_id = msg.leader_id
            logger.info(f"leader is now set to {msg.leader_id}")
            for entry in msg.entries:
                self.log.append(deserialize_command(entry))

            # TODO add in commiting of messages
        else:
            prev_log_term_number = self.log[msg.prev_log_index].term if len(
                self.log) > msg.prev_log_index else None
            logger.warning(f"Append entries failed to append, append entries msg:{msg},\
                current term: {self.term_number} log length {len(self.log)} prev_log_term_number {prev_log_term_number} ")

        resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
            src=self.raft_node.id,
            dst=msg.src,
            term_number=self.term_number,
            match_index=len(self.log),
            success=is_success,
            leader=self.leader_id,
        )
        self.raft_node.send(resp)

    def on_internal_recv_append_entries_response(self, msg: Messages.AppendEntriesResponse):
        logger.critical(
            "Followers and candidates should never receive append entries response")
