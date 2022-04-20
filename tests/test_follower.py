import unittest
import raft_python.messages as Messages
from unittest.mock import Mock
from raft_python.configs import BROADCAST_ALL_ADDR, MAX_DURATION_NO_HEARTBEAT
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower


class TestFollower(unittest.TestCase):
    def setUp(self):
        self.raft_node_mock = Mock(RaftNode)
        self.raft_node_mock.id = 0
        self.raft_node_mock.others = ["1", "2", "3", "4"]
        self.follower_state = Follower(raft_node=self.raft_node_mock)

    def test_randomly_generate_election_timer(self):
        self.assertTrue(MAX_DURATION_NO_HEARTBEAT <= self.follower_state.randomly_generate_election_timer(
        ) <= 2 * MAX_DURATION_NO_HEARTBEAT)

    # TODO: add in leader field test
    def test_on_client_put(self):
        """ Should return redirect and specify leader field if present"""
        incoming_put_req: Messages.PutMessageRequest = Messages.PutMessageRequest(
            "src", self.raft_node_mock.id, "MID", "key", "val", BROADCAST_ALL_ADDR
        )
        redirect_message: Messages.MessageRedirect = Messages.MessageRedirect(
            self.raft_node_mock.id, "src", "MID", BROADCAST_ALL_ADDR)
        self.follower_state.on_client_put(incoming_put_req)
        self.raft_node_mock.send.assert_called_once_with(redirect_message)

    def test_on_client_get(self):
        pass

    def test_on_interal_recv_request_vote(self):
        pass

    def test_on_interal_recv_request_vote_response(self):
        pass


if __name__ == '__main__':
    unittest.main()
