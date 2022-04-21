import unittest
import raft_python.messages as Messages
from unittest.mock import Mock
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower
from raft_python.commands import SetCommand


class TestFollower(unittest.TestCase):
    def setUp(self):
        self.raft_node_mock = Mock(RaftNode)
        self.raft_node_mock.id = 0
        self.raft_node_mock.others = ["1", "2", "3", "4"]
        self.follower_state = Follower(raft_node=self.raft_node_mock)
        self.raft_node_mock.state = self.follower_state

    def test_randomly_generate_election_timer(self):
        self.assertTrue(MAX_DURATION_NO_HEARTBEAT <= self.follower_state.randomly_generate_election_timer(
        ) <= 2 * MAX_DURATION_NO_HEARTBEAT)

    # TODO: add in leader field test
    def test_on_client_put(self):
        """ Should return redirect and specify leader field if present"""
        incoming_put_req: Messages.PutMessageRequest = Messages.PutMessageRequest(
            "src", self.raft_node_mock.id, "MID", "key", "value", BROADCAST_ALL_ADDR
        )
        redirect_message: Messages.MessageRedirect = Messages.MessageRedirect(
            self.raft_node_mock.id, "src", "MID", BROADCAST_ALL_ADDR)
        self.follower_state.on_client_put(incoming_put_req)
        self.raft_node_mock.send.assert_called_once_with(redirect_message)

    def test_on_client_get(self):
        """ Should return redirect and specify leader field if present"""
        incoming_put_req: Messages.PutMessageRequest = Messages.PutMessageRequest(
            "src", self.raft_node_mock.id, "MID", "key", "value", BROADCAST_ALL_ADDR
        )
        redirect_message: Messages.MessageRedirect = Messages.MessageRedirect(
            self.raft_node_mock.id, "src", "MID", BROADCAST_ALL_ADDR)
        self.follower_state.on_client_put(incoming_put_req)
        self.raft_node_mock.send.assert_called_once_with(redirect_message)

    def test_should_accept_vote_success(self):
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            0,
            "voter_src",
            0,
            0
        )
        self.assertTrue(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should have accepted vote success")

    def test_should_accept_vote_fail_already_voted(self):
        self.follower_state.voted_for = "voter_candidate_2"
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            1000,
            "voter_src",
            10,
            999
        )
        self.assertFalse(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should not accept vote since already voted for a candidate")

    def test_should_accept_vote_fail_smaller_term_number(self):
        self.follower_state.term_number = 1000
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            999,
            "voter_src",
            10,
            999
        )
        self.assertFalse(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should not accept vote since term number is smaller")

    def test_should_accept_vote_fail_smaller_last_log_term_number(self):
        self.follower_state.log.append(SetCommand(1000, {}))
        self.follower_state.term_number = 1000
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            1000,
            "voter_src",
            10,
            999
        )
        self.assertFalse(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should not accept vote since message last log term number 999 < 1000")

    def test_should_accept_vote_fail_same_last_log_term_number_shorter_len(self):
        self.follower_state.log.append(SetCommand(1000, {}))
        self.follower_state.log.append(SetCommand(1000, {}))
        self.follower_state.term_number = 1000
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            1000,
            "voter_src",
            1,
            999
        )
        self.assertFalse(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should not accept vote since message last log term number 999 < 1000")

    def test_on_internal_recv_request_vote_success(self):
        """
        Shold reject vote if follower has already voted
        """
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            1000,
            "voter_src",
            10,
            999)
        outgoing_message: Messages.RequestVoteResponse = Messages.RequestVoteResponse(
            self.raft_node_mock.id,
            "voter_src",
            1000,
            True,
        )
        self.follower_state.receive_internal_message(incoming_vote_req)
        self.raft_node_mock.send.assert_called_once_with(outgoing_message)

    def _is_valid_append_entries_req_invalid_term_number(self):
        self.follower_state.term_number = 1
        incoming_vote_req: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
            "voter_src",
            self.raft_node_mock.id,
            0,
            "leader_id",
            10,
            3,
            [],
            3,
            "leader"
        )
        self.assertFalse(
            self.follower_state._is_valid_append_entries_req(incoming_vote_req))

    # TODO: add in more test for is valid append entries


if __name__ == '__main__':
    unittest.main()
