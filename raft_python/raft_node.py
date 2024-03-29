import select
import time
import logging
from copy import deepcopy
from enum import Enum
from typing import List
# from raft_python.states import ALL_NODE_STATES
from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ALL_ADDR, LOGGER_NAME
from raft_python.messages import HelloMessage, get_message_from_payload, IncomingMessageType, MessageRedirect, MessageFail
from raft_python.kv_cache import KVCache
from raft_python.commands import ALL_COMMANDS
from raft_python.stats_recorder import StatsRecorder

logger = logging.getLogger(LOGGER_NAME)


class NodeRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, socket_wrapper: SocketWrapper, kv_cache: KVCache, id: str, others: List[str], stats_recorder: StatsRecorder):
        self.socket: SocketWrapper = socket_wrapper
        # TODO: refactor this to interact with executor interface
        self.executor: KVCache = kv_cache
        self.id: str = id
        self.others: List[str] = others
        self.state = None  # need to register state here
        self.stats_recorder = stats_recorder
        self.pending_messages: dict = {}
        logger.info("Starting Raft Node")

    def send_hello(self):
        hello_msg: HelloMessage = HelloMessage(
            self.id, BROADCAST_ALL_ADDR, BROADCAST_ALL_ADDR)
        logger.info("Replica %s starting up" % self.id)
        self.send(hello_msg)

    def send(self, message: IncomingMessageType, tag: str = None):
        """Wrapper to call internal socket Manager to send message"""
        logger.debug(
            f"sending msg : src {message.src} dst {message.dst} type {message.type} leader {message.leader}")
        stat_name = message.name
        if tag is not None:
            stat_name = f"{message.name}_{tag}"
        self.stats_recorder.inc_stat(stat_name, 1)
        self.socket.send(message.serialize())

    # State wrapper functions
    def register_state(self, state: "ALL_NODE_STATES"):
        self.state = state

    def change_state(self, new_state: "ALL_NODE_STATES"):
        logger.info(
            f"State changed from {self.state.name} to {new_state.name}")
        new_created_state: "ALL_NODE_STATES" = new_state(self.state, self)
        self.state.destroy()
        logger.info(f"self pending dict:{self.pending_messages}")
        self.state = new_created_state
        return new_created_state

    # KV Store execute wrapper
    def execute(self, command: ALL_COMMANDS):
        return self.executor.execute(command)

    def send_pending_messages_redirect(self):
        if self.state.leader_id != BROADCAST_ALL_ADDR:
            for client_req in self.pending_messages.values():
                msg_redirect: MessageRedirect = MessageRedirect(
                    src=self.id,
                    dst=client_req.dst,
                    MID=client_req.MID,
                    leader=self.state.leader_id
                )
                logger.info(
                    f"[REDIRECT] Sending message redirect:{msg_redirect.MID}")
                self.send(msg_redirect)
            self.pending_messages = {}

    def _run_single_step(self, timeout):
        # check if there are any looping messages
        if self.state.node_raft_command is not None:
            # execute any commands
            if time.time() > self.state.execution_time:
                # execute here
                if self.state.args is not None:
                    self.state.node_raft_command(self.state.args)
                else:
                    self.state.node_raft_command()

        # make socket connection non-blocking
        socket = select.select([self.socket.socket], [], [], 0.1)[0]
        for _ in socket:
            msg = self.socket.receive()
            req: IncomingMessageType = get_message_from_payload(msg)
            self.state.receive_message(req)
            logger.debug(f"received message :{req.serialize()}")

        self.send_pending_messages_redirect()

    def run(self, timeout=None):
        # used to simulate in integration tests
        curr_time = time.time()
        while timeout is None or time.time() < curr_time + timeout:
            self._run_single_step(timeout)
            logger.debug(
                f"stats of messages sent:{self.stats_recorder.get_stats()}")
            logger.info(
                f"self state : {type(self.state)} term_number:{self.state.term_number} log length:{len(self.state.log)} commit_ix:{self.state.commit_index} self.leader_id {self.state.leader_id} pending_message {self.pending_messages}")
