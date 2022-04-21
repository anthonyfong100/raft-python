import time
import logging
import raft_python.messages as Messages
from raft_python.configs import LOGGER_NAME, HEARTBEAT_INTERNVAL
from raft_python.states.state import State
logger = logging.getLogger(LOGGER_NAME)


class Leader(State):
    def __init__(self, old_state: "Candidate" = None, raft_node: "RaftNode" = None):
        super().__init__(old_state, raft_node)
        logging.critical(f"Leader with Term:{self.term_number}")
        self.leader_id = self.raft_node.id
        self.node_raft_command = self.send_heartbeat
        self.execution_time = self.last_hearbeat + HEARTBEAT_INTERNVAL
        self.args = None

        self.match_index = {node: 0 for node in self.cluster_nodes}
        self.next_index = {node: 0 for node in self.cluster_nodes}
        self.commit_index = 0
        self.send_heartbeat()

    # TODO: Modfiy this to call in a loop
    def append_entries(self, is_heartbeat=False):
        for peer in self.cluster_nodes:
            prev_log_index: int = self.next_index[peer]
            prev_log_term: int = self.log[prev_log_index].term_number if len(
                self.log) > prev_log_index else 0
            msg: Messages.AppendEntriesReq = Messages.AppendEntriesReq(
                src=self.raft_node.id,
                dst=peer,
                term_number=self.term_number,
                leader_id=self.raft_node.id,
                prev_log_index=prev_log_index,
                prev_log_term_number=prev_log_term,
                entries=[] if is_heartbeat else [],  # TODO: fix this []
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
        logger.critical("sending heartbeat")
        self.append_entries(is_heartbeat=True)
        self.last_hearbeat = time.time()
        self.execution_time = self.last_hearbeat + HEARTBEAT_INTERNVAL

    # TODO: Remove sending heartbeats
    def destroy(self):
        return

    def _register_loop_send_heartbeat(self):
        """Regularly send out heartbeat messages"""
        pass

    # TODO: send output
    def on_client_put(self, msg: Messages.PutMessageRequest):
        logger.debug(f"Received put request: {msg.serialize()}")
        pass

    # TODO: send redirect message
    def on_client_get(self, msg: Messages.GetMessageRequest):
        pass

    def on_internal_recv_request_vote(self, msg: Messages.RequestVote):
        pass

    def on_internal_recv_request_vote_response(self, msg: Messages.RequestVoteResponse):
        pass
