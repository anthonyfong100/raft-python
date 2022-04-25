from email.message import Message
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

    def test_on_client_put(self):
        put_message1: Messages.PutMessageRequest = Messages.PutMessageRequest(
            src="client",
            dst=self.leader_state.raft_node.id,
            MID="MID1",
            key="key1",
            value="value1",
            leader="FFFF"
        )
        self.leader_state.on_client_put(put_message1)
        self.assertEqual(len(self.leader_state.log), 1)
        self.assertDictEqual(self.leader_state.match_index, {
            "0": -1,
            self.leader_state.raft_node.id: 0,
            "2": -1,
            "3": -1,
        })
        self.assertIsNotNone(
            self.leader_state.waiting_client_response.get(put_message1.MID, None), "should add to put message dict")

        append_entries_reqs: List[Messages.AppendEntriesReq] = [
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "0", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "2", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "3", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,))
        ]
        self.raft_node_mock.send.assert_has_calls(
            append_entries_reqs, any_order=True)

    def test_on_client_get(self):
        put_message1: Messages.GetMessageRequest = Messages.GetMessageRequest(
            src="client",
            dst=self.leader_state.raft_node.id,
            MID="MID1",
            key="key1",
            leader="FFFF"
        )
        self.leader_state.on_client_get(put_message1)
        self.assertEqual(len(self.leader_state.log), 1)
        self.assertDictEqual(self.leader_state.match_index, {
            "0": -1,
            self.leader_state.raft_node.id: 0,
            "2": -1,
            "3": -1,
        })
        self.assertIsNotNone(
            self.leader_state.waiting_client_response.get(put_message1.MID, None), "should add to put message dict")

        append_entries_reqs: List[Messages.AppendEntriesReq] = [
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "0", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "2", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,)),
            call(Messages.AppendEntriesReq(self.raft_node_mock.id, "3", self.leader_state.term_number,
                 self.raft_node_mock.id, -1, 0, self.leader_state.log, self.leader_state.commit_index, self.raft_node_mock.id,))
        ]
        self.raft_node_mock.send.assert_has_calls(
            append_entries_reqs, any_order=True)

    def test_on_internal_recv_append_entries_response_success(self):
        """ Send append entries to node 3 receive success"""
        self.leader_state.log.append(SetCommand(1, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(2, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(3, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(4, {}, MID="MID"))
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
            call(SetCommand(1, {}, MID="MID")),
            call(SetCommand(2, {}, MID="MID")),
            call(SetCommand(3, {}, MID="MID")),
        ]
        self.raft_node_mock.execute.assert_has_calls(
            execute_commands_mock)
        self.assertEqual(self.leader_state.commit_index, 2,
                         "Commit index should be set to 2")

    def test_on_internal_recv_append_entries_response_failure(self):
        """ Send append entries to node 3 receive failure"""
        self.leader_state.log.append(SetCommand(1, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(2, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(3, {}, MID="MID"))
        self.leader_state.log.append(SetCommand(4, {}, MID="MID"))
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

    def test_on_internal_recv_append_entries_response_send_reply(self):
        """ Send append entries to node 3 receive success"""
        put_message1: Messages.PutMessageRequest = Messages.PutMessageRequest(
            src="client",
            dst=self.leader_state.raft_node.id,
            MID="MID1",
            key="key1",
            value="value1",
            leader="FFFF"
        )
        self.leader_state.on_client_put(put_message1)
        for other_raft_node_id in ["0", "1"]:
            append_entry_resp: Messages.AppendEntriesResponse = Messages.AppendEntriesResponse(
                src=other_raft_node_id,
                dst=self.leader_state.raft_node.id,
                term_number=self.leader_state.term_number,
                match_index=0,
                success=True,
                leader=self.leader_state.leader_id,
            )

            self.leader_state.on_internal_recv_append_entries_response(
                append_entry_resp)

        # once receive majority 3 votes --> should commit and send reply
        self.raft_node_mock.execute.assert_called_once_with(
            SetCommand(
                term_number=self.leader_state.term_number,
                args={"key": put_message1.key, "value": put_message1.value},
                MID=put_message1.MID))
        self.raft_node_mock.send.assert_called_with(Messages.PutMessageResponseOk(
            src=self.leader_state.raft_node.id,
            dst=put_message1.src,
            MID=put_message1.MID,
            leader=self.raft_node_mock.id
        ))
        self.assertIsNone(
            self.leader_state.waiting_client_response.get(put_message1.MID, None))


if __name__ == '__main__':
    unittest.main()
