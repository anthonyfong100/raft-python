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


class TestLeader(unittest.TestCase):
    def setUp(self):
        self.raft_node_mock = Mock(RaftNode)
        self.raft_node_mock.id = 1
        self.raft_node_mock.others = ["0", "2", "3"]
        self.follower_state = Follower(raft_node=self.raft_node_mock)
        self.candidate_state = Candidate(self.follower_state)
        self.leader_state = Leader(self.candidate_state)
        self.raft_node_mock.state = self.leader_state

    def test_heartbeat(self):
        before_heartbeat_call_count = self.raft_node_mock.send.call_count
        append_entries_reqs: List[Messages.AppendEntriesReq] = [
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "0", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, [], self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "2", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, [], self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "3", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, [], self.leader_state.commit_index, self.raft_node_mock.id,))
        ]
        self.leader_state.send_heartbeat()
        after_heartbeat_call_count = self.raft_node_mock.send.call_count
        self.raft_node_mock.send.assert_has_calls(
            append_entries_reqs, any_order=True)
        self.assertEqual(after_heartbeat_call_count - before_heartbeat_call_count,
                         len(self.raft_node_mock.others))

    def test_on_internal_recv_append_entries_response_success(self):
        """ Send append entries to node 3 receive success"""
        self.leader_state.log.append(SetCommand(1, {}))
        self.leader_state.log.append(SetCommand(2, {}))
        self.leader_state.log.append(SetCommand(3, {}))
        self.leader_state.log.append(SetCommand(4, {}))
        self.leader_state.commit_index = -1
        self.leader_state.match_index = {
            "0": 1,
            self.leader_state.raft_node.id: 4,  # length of current log
            "2": 2,
            "3": 2,
        }
        append_entry_resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
            src="3",
            dst=self.leader_state.raft_node.id,
            term_number=self.leader_state.term_number,
            match_index=3,
            success=True,
            leader=self.leader_state.leader_id,
        )

        self.leader_state.on_internal_recv_append_entries_response(
            append_entry_resp)

        # should increment match index for node 3
        self.assertEqual(self.leader_state.match_index["3"], 3)
        # current match index 1 2 3 4 --> should commit to index 2 since index 2 has 3 node acknowledging it
        execute_commands_mock: List[SetCommand] = [
            call(SetCommand(1, {})),
            call(SetCommand(2, {})),
            call(SetCommand(3, {})),
        ]
        self.raft_node_mock.execute.assert_has_calls(
            execute_commands_mock)

        self.leader_state.commit_index = 3

    def test_on_internal_recv_append_entries_response_failure(self):
        """ Send append entries to node 3 receive failure"""
        self.leader_state.log.append(SetCommand(1, {}))
        self.leader_state.log.append(SetCommand(2, {}))
        self.leader_state.log.append(SetCommand(3, {}))
        self.leader_state.log.append(SetCommand(4, {}))
        self.leader_state.commit_index = -1
        self.leader_state.match_index = {
            "0": 1,
            self.leader_state.raft_node.id: 4,  # length of current log
            "2": 2,
            "3": 2,
        }
        append_entry_resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
            src="3",
            dst=self.leader_state.raft_node.id,
            term_number=self.leader_state.term_number,
            match_index=3,
            success=False,
            leader=self.leader_state.leader_id,
        )

        self.leader_state.on_internal_recv_append_entries_response(
            append_entry_resp)

        # should decrement match index for node 3
        self.assertEqual(self.leader_state.match_index["3"], 1)


if __name__ == '__main__':
    unittest.main()
