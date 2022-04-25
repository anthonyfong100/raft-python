import time
import logging
import raft_python.messages as Messages
import statistics
from typing import List, Optional, Union
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
        self.waiting_client_response: dict[int,
                                           Union[Messages.PutMessageResponseOk, Messages.GetMessageResponseOk]] = {}
        self.send_heartbeat()

    def _reset_timeout(self):
        """ 
        Resets the last heartbeat, randomize the number election timer and generate the next execution time for voting
        """
        self.last_hearbeat = time.time()
        self.execution_time = self.last_hearbeat + HEARTBEAT_INTERNVAL

    def send_append_entries(self):
        for peer in self.cluster_nodes:
            # dont send to yourself
            if peer == self.raft_node.id:
                continue
            prev_log_index: int = self.match_index[peer]
            prev_log_term: int = self.log[prev_log_index].term_number if len(
                self.log) > prev_log_index and prev_log_index != -1 else 0
            entries: List[ALL_COMMANDS]
            if prev_log_index == -1:
                entries = self.log.copy()
            else:
                entries = self.log[prev_log_index + 1:].copy()

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
            self.raft_node.send(msg)
        self._reset_timeout()

    # TODO: remove this and use send append entries only
    def send_heartbeat(self):
        self.send_append_entries()

    # TODO: Remove sending heartbeats
    def destroy(self):
        for node_id, client_req in self.waiting_client_response.items():
            logger.warning(
                f"node id:{node_id} has unanswered response: {client_req.serialize()}")
        return

    # TODO: Do this the right way by waiting for quorum
    def on_client_put(self, msg: Messages.PutMessageRequest):
        logger.debug(f"Received put request: {msg.serialize()}")

        # create a new command and put it in
        set_command: SetCommand = SetCommand(
            term_number=self.term_number,
            args={
                "key": msg.key,
                "value": msg.value,
            },
            MID=msg.MID,
        )
        self.log.append(set_command)
        self.match_index[self.raft_node.id] = len(self.log) - 1
        put_response_ok: Messages.PutMessageResponseOk = Messages.PutMessageResponseOk(
            self.raft_node.id,
            msg.src,
            msg.MID,
            self.leader_id
        )
        self.waiting_client_response[msg.MID] = put_response_ok
        self.send_append_entries()

    # TODO: Do this the right way by waiting for quorum
    def on_client_get(self, msg: Messages.GetMessageRequest):
        logger.debug(f"Received get request: {msg.serialize()}")

        # create a new command and put it in
        get_command: GetCommand = GetCommand(
            term_number=self.term_number,
            args={
                "key": msg.key,
            },
            MID=msg.MID,
        )
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
        logger.warning("Leader should never receive append entries call")
        pass

    def on_internal_recv_append_entries_response(self, msg: Messages.AppendEntriesResponse):
        """
        Upon receiving confirmation from other raft nodes
        """
        if msg.success:
            self.match_index[msg.src] = msg.match_index
            self.match_index[self.raft_node.id] = len(self.log) - 1
            index = statistics.median_low(self.match_index.values())

            for ix_commit in range(self.commit_index + 1, index + 1):
                logger.debug(
                    f"commiting {ix_commit} self.log:{self.log} self.match index:{self.match_index}")
                command: ALL_COMMANDS = self.log[ix_commit]
                resp_value = self.raft_node.execute(command)
                self.commit_index = index

                # send client response if there is a response expected
                resp_packet = self.waiting_client_response.get(
                    command.MID, None)
                if resp_packet is not None:
                    self.raft_node.send(resp_packet)
                    # set waiting call to be none
                    del (self.waiting_client_response[command.MID])

            self.commit_index = index  # update the commit index

        else:
            # decremeent the next index for that receiver
            self.match_index[msg.src] = max(-1, self.match_index[msg.src] - 1)
        logger.debug(
            f"leader log legnth:{len(self.log)} leader commit index:{self.commit_index}, match index:{self.match_index} ")
