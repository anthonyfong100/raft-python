import unittest
import raft_python.messages as Messages
from typing import List
from unittest.mock import Mock, call
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower
from raft_python.states.candidate import Candidate
from raft_python.commands import SetCommand
from raft_python.states.leader import Leader


class TestCandidate(unittest.TestCase):
    def setUp(self):
        self.raft_node_mock = Mock(RaftNode)
        self.raft_node_mock.id = 1
        self.raft_node_mock.others = ["0", "2", "3"]
        self.follower_state = Follower(raft_node=self.raft_node_mock)
        self.candidate_state = Candidate(self.follower_state)
        self.raft_node_mock.state = self.candidate_state

    def test_run_elections(self):
        prev_term_number: int = self.follower_state.term_number
        last_log_index: int = len(self.candidate_state.log)
        last_log_term: int = self.candidate_state.log[-1].term if self.candidate_state.log else 0
        vote_reqs: List[Messages.RequestVote] = [
            call(Messages.RequestVote(self.raft_node_mock.id, "0", self.candidate_state.term_number, self.raft_node_mock.id,
                                      last_log_index, last_log_term, self.candidate_state.leader_id)),
            call(Messages.RequestVote(self.raft_node_mock.id, "2", self.candidate_state.term_number, self.raft_node_mock.id,
                                      last_log_index, last_log_term, self.candidate_state.leader_id)),
            call(Messages.RequestVote(self.raft_node_mock.id, "3", self.candidate_state.term_number, self.raft_node_mock.id,
                                      last_log_index, last_log_term, self.candidate_state.leader_id)),
        ]
        self.assertEqual(self.candidate_state.term_number, prev_term_number + 1,
                         "Should increment term number when running elections")
        self.assertEqual(self.candidate_state.voted_for, self.raft_node_mock.id,
                         "voted_for should be its own raft_node id")
        self.assertEqual(self.candidate_state.vote_count, 1,
                         "Should vote for iteself")
        self.raft_node_mock.send.assert_has_calls(vote_reqs, any_order=True)
        self.assertEqual(self.raft_node_mock.send.call_count,
                         len(self.candidate_state.cluster_nodes))

    def test_on_internal_recv_request_vote_response_success(self):
        """
         On receiving a vote need to increment count and call change state if got majority
        """
        self.candidate_state.term_number = 10
        incoming_vote_resp: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            "raft_node1", self.raft_node_mock.id, 9, True, self.candidate_state.leader_id
        )

        self.candidate_state.receive_internal_message(incoming_vote_resp)
        self.assertEqual(self.candidate_state.vote_count, 2)
        self.raft_node_mock.change_state.assert_not_called()
        self.candidate_state.receive_internal_message(incoming_vote_resp)
        self.assertEqual(self.candidate_state.vote_count, 3)
        self.raft_node_mock.change_state.assert_called_once_with(Leader)

    def test_on_internal_recv_request_vote_response_change_follower(self):
        """
         On receiving a vote with higher term number change from candidate to follower
        """
        self.candidate_state.term_number = 8
        incoming_vote_resp: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            "raft_node1", self.raft_node_mock.id, 9, True, self.candidate_state.leader_id
        )

        self.candidate_state.receive_internal_message(incoming_vote_resp)
        self.raft_node_mock.change_state.assert_called_once_with(Follower)


if __name__ == '__main__':
    unittest.main()
