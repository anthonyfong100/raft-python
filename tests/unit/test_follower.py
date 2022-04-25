import unittest
import raft_python.messages as Messages
from unittest.mock import Mock
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower
from raft_python.commands import SetCommand


# Unit test for follower
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
            0,
            "voter_src",
            10,
            999
        )
        self.assertFalse(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should not accept vote since already voted for a candidate")

    def test_should_accept_vote_success_already_voted_but_higher_term_number(self):
        self.follower_state.voted_for = "voter_candidate_2"
        incoming_vote_req: Messages.RequestVote = Messages.RequestVote(
            "voter_src",
            self.raft_node_mock.id,
            10,
            "voter_src",
            10,
            999
        )
        self.assertTrue(self.follower_state._should_accept_vote(
            incoming_vote_req), "Should accept vote since term number is higher")

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
        self.follower_state.log.append(SetCommand(1000, {}, MID="MID"))
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
        self.follower_state.log.append(SetCommand(1000, {}, MID="MID"))
        self.follower_state.log.append(SetCommand(1000, {}, MID="MID"))
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

    def test_is_valid_append_entries_req_invalid_term_number(self):
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
    def test_is_valid_append_entries_req_invalid_prev_log_index(self):
        """
        Check if is valid append entries
        1. length of log in recv > prev log index
        2. log entry term number at prev log index is the same is valid
        """
        invalid_incoming_vote_req: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
            src="voter_src",
            dst=self.raft_node_mock.id,
            term_number=1,
            leader_id="leader_id",
            prev_log_index=1,
            prev_log_term_number=3,
            entries=[],
            leader_commit_index=3,
            leader="leader"
        )
        self.assertFalse(
            self.follower_state._is_valid_append_entries_req(invalid_incoming_vote_req))

    def test_is_valid_append_entries_req_invalid_prev_log_term(self):
        """
        Check if is valid append entries
        1. length of log in recv > prev log index
        2. log entry term number at prev log index is the same is valid
        """
        self.follower_state.log.append(SetCommand(4, {}, MID="MID"))
        invalid_incoming_vote_req: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
            src="voter_src",
            dst=self.raft_node_mock.id,
            term_number=1,
            leader_id="leader_id",
            prev_log_index=0,
            prev_log_term_number=3,
            entries=[],
            leader_commit_index=3,
            leader="leader"
        )
        self.assertFalse(
            self.follower_state._is_valid_append_entries_req(invalid_incoming_vote_req))

    def test_on_internal_recv_append_entries(self):
        """
        Check if is valid append entries
        1. length of log in recv > prev log index
        2. log entry term number at prev log index is the same is valid
        """
        self.follower_state.log.append(SetCommand(3, {}, MID="MID"))
        valid_incoming_append_entry_req: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
            src="voter_src",
            dst=self.raft_node_mock.id,
            term_number=1,
            leader_id="leader_id",
            prev_log_index=0,
            prev_log_term_number=3,
            entries=[SetCommand(
                4, {"key", "a", "value", "b"}, MID="MID")],
            leader_commit_index=0,
            leader="leader"
        )
        self.assertTrue(
            self.follower_state._is_valid_append_entries_req(valid_incoming_append_entry_req))
        self.follower_state.receive_internal_message(
            valid_incoming_append_entry_req)
        self.assertEqual(self.follower_state.log, [SetCommand(3, {}, MID="MID"), SetCommand(
            4, {"key", "a", "value", "b"}, MID="MID")])

        self.raft_node_mock.execute.assert_called_once_with(
            SetCommand(3, {}, MID="MID"))

        append_entry_resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
            src=self.follower_state.raft_node.id,
            dst="voter_src",
            term_number=1,
            match_index=1,
            success=True,
            leader="leader_id",
        )
        self.raft_node_mock.send.assert_called_once_with(append_entry_resp)


if __name__ == '__main__':
    unittest.main()
