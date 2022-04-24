import logging
import raft_python.messages as Messages
from raft_python.messages import RequestVote
from raft_python.states.follower import Follower
from raft_python.states.leader import Leader
from raft_python.configs import LOGGER_NAME, BROADCAST_ALL_ADDR
logger = logging.getLogger(LOGGER_NAME)


class Candidate(Follower):
    name = "Candidate"

    def __init__(self, old_state: Follower = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        self.voted_for = None
        self.vote_count = 0
        self.election_timer = self.randomly_generate_election_timer()
        self.run_elections()

        self.node_raft_command = self.raft_node.change_state
        self.execution_time = self.last_hearbeat + self.election_timer
        self.args = (Candidate)

    def run_elections(self):
        logger.info("Running for elections")
        self.term_number += 1
        self.voted_for = self.raft_node.id
        last_log_index: int = len(self.log)
        last_log_term_number: int = self.log[-1].term_number if self.log else 0
        for other_node_id in self.cluster_nodes:
            request_vote: RequestVote = RequestVote(
                self.raft_node.id,
                other_node_id,
                self.term_number,
                self.raft_node.id,
                last_log_index,
                last_log_term_number,
                BROADCAST_ALL_ADDR)  # TODO: Check if should broadcast leader as unknown
            self.raft_node.send(request_vote)

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        self.vote_count += msg.vote_granted
        logger.info(
            f"Received vote result of {msg.serialize()} new_vote_count:{self.vote_count}"
        )
        if self.vote_count > len(self.cluster_nodes) / 2:
            self.raft_node.change_state(Leader)

    def on_internal_recv_append_entries(self, msg: Messages.AppendEntriesReq):
        self.raft_node.change_state(Follower)
        self.raft_node.state.on_internal_recv_append_entries(msg)
