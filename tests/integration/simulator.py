import json
import select
import socket
import time
import threading
from typing import Deque, Dict, List
from raft_python.configs import BUFFER_SIZE
from raft_python.raft_node import RaftNode
from raft_python.states.follower import Follower
from collections import deque
from raft_python.raft_node import RaftNode
from raft_python.socket_wrapper import SocketWrapper
from raft_python.kv_cache import KVCache
from unittest.mock import Mock


def wait_and_run(func, wait_timeout=3, run_timeout=30):
    time.sleep(wait_timeout)
    return func(run_timeout)


class Simulator:
    def __init__(self, num_nodes: int):
        self.node_socket = {}
        self.simulator_socket = {}
        self.kv_cache = {}
        self.raft_node_mapping: Dict[str, RaftNode] = {}
        self.num_nodes = num_nodes

        self.setup_simulator(num_nodes)

    def setup_raft_node(self, node_id: str, other_nodes_in_network: List[str]):
        # create simulator
        simulator_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        simulator_socket.bind(('localhost', 0))
        port = simulator_socket.getsockname()[1]

        # create raft node socket
        node_socket = SocketWrapper(port)

        # register both sockets
        self.node_socket[node_id] = node_socket
        self.simulator_socket[node_id] = simulator_socket

        # create kv cache
        self.kv_cache[node_id] = KVCache()

        # create state machine mocks
        raft_node = RaftNode(
            node_socket, self.kv_cache[node_id], node_id, other_nodes_in_network)
        follower_state: Follower = Follower(raft_node=raft_node)
        raft_node.register_state(follower_state)
        self.raft_node_mapping[node_id] = raft_node

    def setup_simulator(self, num_nodes: int):
        """ Create all required mocks"""
        for node_id in range(num_nodes):
            other_nodes_id_list = [str(id)
                                   for id in range(num_nodes) if id != node_id]
            self.setup_raft_node(str(node_id), other_nodes_id_list)

    def listen_and_forward(self, run_timeout):
        """ Listen to incoming messages and forward"""
        curr_time = time.time()

        while time.time() < curr_time + run_timeout:
            sockets = select.select(
                self.simulator_socket.values(), [], [], 0.1)[0]
            for socket in sockets:
                data, _ = socket.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode('utf-8'))
                print(msg)

    def run(self, wait_timeout=3, run_timeout=30):
        threads = []
        for node_id in range(self.num_nodes):
            threads.append(threading.Thread(
                target=wait_and_run,
                args=(self.raft_node_mapping[str(
                    node_id)].run, wait_timeout, run_timeout)
            ))

        # run listen and forward messages in a seperate thread
        # run for total duration of wait timeout plus run timeout
        threads.append(threading.Thread(
            target=self.listen_and_forward,
            args=(wait_timeout + run_timeout,)
        ))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        return

    def teardown(self):
        all_sockets = list(self.node_socket.values()) + \
            list(self.simulator_socket.values())
        for socket in all_sockets:
            socket.close()