import time
import logging
import random
import raft_python.messages as Messages
from raft_python.states.state import State
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT, LOGGER_NAME
logger = logging.getLogger(LOGGER_NAME)


class Follower(State):
    name = "Follower"

    def __init__(self, old_state: State = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        self.voted_for = None
        self.leader_id = BROADCAST_ALL_ADDR
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
        # check if term is okay
        is_valid_msg_term_number: bool = msg.term_number > self.term_number
        is_valid_candidate: bool = msg.term_number == self.term_number and self.voted_for in [
            None, msg.candidate_id]
        term_ok: bool = is_valid_msg_term_number or is_valid_candidate

        # check if last log term is ok

        last_log_term_number_follower: int = self.log[-1].term_number if self.log else 0
        is_valid_last_log_term_number: bool = (msg.last_log_term_number > last_log_term_number_follower) or \
            (msg.last_log_term_number == last_log_term_number_follower and
             msg.last_log_index >= len(self.log))
        logger.info(
            f"should accept vote {is_valid_msg_term_number} {is_valid_candidate} {is_valid_last_log_term_number}"
            f" self.term_number {self.term_number} self.voted_for {self.voted_for} log length {len(self.log)}"
            f" last_log_term_number_follower {last_log_term_number_follower}\n"
            f" msg last log term number {msg.last_log_term_number} msg last log index {msg.last_log_index}")
        return term_ok and is_valid_last_log_term_number

    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        """Decide if we approve the new leader request"""
        should_accept_vote: bool = self._should_accept_vote(msg)
        if should_accept_vote:
            # change state to follower
            new_state = self
            # message not sent from iteself
            if type(self) != Follower and msg.src != self.raft_node.id:
                new_state = self.raft_node.change_state(Follower)
            # https://courses.grainger.illinois.edu/ece428/sp2020//assets/slides/lect17.pdf
            new_state.term_number = msg.term_number
            new_state.voted_for = msg.candidate_id
            new_state._reset_timeout()
            logger.info(
                f"Voting for {msg.candidate_id} self term_number:{self.term_number} self ")

        vote_response: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            self.raft_node.id, msg.src,
            self.term_number,
            should_accept_vote,
            self.leader_id
        )
        self.raft_node.send(vote_response)

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        logger.warning(f"voter note received vote response")
        pass

    def _is_valid_append_entries_req(self, msg: Messages.AppendEntriesReq):
        """
        Check if term number in message > current term number
        Check if term number at prev log index == msg prev log term number
        """
        term_number_is_valid: bool = msg.term_number >= self.term_number
        log_ok = len(self.log) > msg.prev_log_index
        if log_ok and msg.prev_log_index >= 0:  # when just initialize prev_log_index = -1
            # compare the last term
            log_ok = self.log[msg.prev_log_index].term_number == msg.prev_log_term_number
        return term_number_is_valid and log_ok

    def on_internal_recv_append_entries(self, msg: Messages.AppendEntriesReq):
        logger.info(
            f"receive append entry from :{msg.src} term_number:{msg.term_number} prev_log_index:{msg.prev_log_index}"
            f"prev log term: {msg.prev_log_term_number}")
        if msg.term_number >= self.term_number:
            # reset election counter
            self._reset_timeout()
            self.term_number = msg.term_number
            self.voted_for = None
        is_success: bool = self._is_valid_append_entries_req(msg)
        if is_success:
            # remove log terms from index onwards
            self.log = self.log[:msg.prev_log_index + 1].copy()
            self.leader_id = msg.leader_id
            logger.debug(f"leader is now set to {msg.leader_id}")
            for entry in msg.entries:
                self.log.append(entry)

            max_commit_index = min(len(self.log)-1, msg.leader_commit_index)
            if max_commit_index != msg.leader_commit_index:
                logger.warning(f"log is behind leader! self.log length:{len(self.log)}"
                               f" leader.commit_index: {msg.leader_commit_index}")
            for commit_ix in range(self.commit_index + 1, max_commit_index + 1):
                self.raft_node.execute(self.log[commit_ix])
            self.commit_index = max_commit_index
        else:
            logger.info(
                f"self.log:{len(self.log)} prev log index:{msg.prev_log_index}")
            prev_log_term_number = self.log[msg.prev_log_index].term_number if len(
                self.log) > msg.prev_log_index and len(self.log) != 0 else None
            logger.warning(f"Append entries failed to append, append entries msg term_number:{msg.term_number} "
                           f"msg prev_log_index:{msg.prev_log_index}, msg prev_log_term:{msg.prev_log_term_number} \n"
                           f"current term number: {self.term_number} log length: {len(self.log)} "
                           f"prev_log_term_number: {prev_log_term_number}")

        resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
            src=self.raft_node.id,
            dst=msg.src,
            term_number=self.term_number,
            match_index=len(self.log) - 1,
            success=is_success,
            leader=self.leader_id,
        )
        self.raft_node.send(resp)

    def on_internal_recv_append_entries_response(self, msg: Messages.AppendEntriesResponse):
        logger.warning(
            "Followers and candidates should never receive append entries response")
