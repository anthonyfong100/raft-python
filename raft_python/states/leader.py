import time
import logging
import raft_python.messages as Messages
from typing import List, Optional
from raft_python.configs import LOGGER_NAME, HEARTBEAT_INTERNVAL
from raft_python.states.state import State
from raft_python.commands import ALL_COMMANDS, SetCommand, GetCommand
logger = logging.getLogger(LOGGER_NAME)


class Leader(State):
    name = "Leader"

    def __init__(self, old_state: "Candidate" = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        logger.info(f"Leader with Term:{self.term_number}")
        self.leader_id = self.raft_node.id
        self.node_raft_command = self.send_heartbeat
        self.execution_time = self.last_hearbeat + HEARTBEAT_INTERNVAL
        self.args = None

        self.match_index = {node: -1 for node in self.cluster_nodes}
        self.next_index = {node: -1 for node in self.cluster_nodes}
        self.commit_index = -1  # store the last index of command executed in log
        self.send_heartbeat()

    # TODO: Modfiy this to call in a loop
    def append_entries(self, is_heartbeat=False):
        for peer in self.cluster_nodes:
            # dont send to yourself
            if peer == self.raft_node.id:
                continue
            prev_log_index: int = self.next_index[peer]
            prev_log_term: int = self.log[prev_log_index].term_number if len(
                self.log) > prev_log_index and prev_log_index != -1 else 0
            entries: List[ALL_COMMANDS]
            if is_heartbeat:
                entries = []
            elif prev_log_index == -1:
                entries = self.log
            else:
                entries = self.log[prev_log_index:]

            msg: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
                src=self.raft_node.id,
                dst=peer,
                term_number=self.term_number,
                leader_id=self.raft_node.id,
                prev_log_index=prev_log_index,
                prev_log_term_number=prev_log_term,
                entries=entries,
                leader_commit_index=self.commit_index,
                leader=self.raft_node.id,
            )
            logger.debug(
                f"Making AppendEntriesRPC call with {msg.serialize()}")

            self.raft_node.send(msg)

        # timeout = randrange(1, 4) * 10 ** (-1 if config.debug else -2)
        # loop = asyncio.get_event_loop()
        # self.append_timer = loop.call_later(timeout, self.send_append_entries)

    def send_heartbeat(self):
        self.append_entries(is_heartbeat=True)
        self.last_hearbeat = time.time()
        self.execution_time = self.last_hearbeat + HEARTBEAT_INTERNVAL

    # TODO: Remove sending heartbeats
    def destroy(self):
        return

    def _register_loop_send_heartbeat(self):
        """Regularly send out heartbeat messages"""
        pass

    # TODO: Do this the right way by waiting for quorum
    def on_client_put(self, msg: Messages.PutMessageRequest):
        logger.debug(f"Received put request: {msg.serialize()}")

        # create a new command and put it in
        set_command: SetCommand = SetCommand(
            self.term_number, {
                "key": msg.key,
                "value": msg.value
            }
        )
        self.log.append(set_command)
        self.raft_node.execute(set_command)

        put_response_ok: Messages.PutMessageResponseOk = Messages.PutMessageResponseOk(
            self.raft_node.id,
            msg.src,
            msg.MID,
            self.leader_id
        )
        self.raft_node.send(put_response_ok)

    # TODO: Do this the right way by waiting for quorum
    def on_client_get(self, msg: Messages.GetMessageRequest):
        logger.debug(f"Received put request: {msg.serialize()}")

        # create a new command and put it in
        get_command: GetCommand = GetCommand(
            self.term_number, {
                "key": msg.key,
            }
        )
        self.log.append(get_command)
        value: Optional[str] = self.raft_node.execute(get_command)
        get_response_ok: Messages.GetMessageResponseOk = Messages.GetMessageResponseOk(
            self.raft_node.id,
            msg.src,
            msg.MID,
            value if value is not None else "",
            self.leader_id
        )
        self.raft_node.send(get_response_ok)

    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        pass

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass

    def on_internal_recv_append_entries(self, msg: Messages.AppendEntriesReq):
        logger.critical("Leader should never receive append entries call")
        pass

    def on_internal_recv_append_entries_response(self, msg: Messages.AppendEntriesResponse):
        """
        Upon receiving confirmation from other raft nodes
        """
        # if msg.success:
        #     self.match_index[msg.src] = msg.match_index
        #     self.next_index[msg.src] = msg.match_index + 1
        #     self.match_index[self.raft_node.id] = len(self.log)
        #     self.next_index[self.raft_node.id] = len(self.log) + 1
        #     index = statistics.median_low(self.match_index.values())

        #     for ix_commit in range(self.commit_index + 1, index + 1):
        #         self.raft_node.execute(self.log[ix_commit])

        #     # TODO: send client resposne after successful so they can update their log
        #     # self.send_client_append_response()
        # else:
        #     # decremeent the next index for that receiver
        #     self.nextIndex[msg.src] = max(0, self.nextIndex[msg.src] - 1)
