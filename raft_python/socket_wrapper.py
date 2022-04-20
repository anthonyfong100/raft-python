import socket
import json
from raft_python.configs import BUFFER_SIZE


class SocketWrapper:
    def __init__(self, port: int):
        self.port = port
        self.socket = self._setup_socket()

    def _setup_socket(self):
        _socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _socket.bind(('localhost', 0))
        return _socket

    def send(self, message: dict):
        self.socket.sendto(json.dumps(message).encode(
            'utf-8'), ('localhost', self.port))

    def receive(self, buff_size=BUFFER_SIZE) -> dict:
        data, _ = self.socket.recvfrom(buff_size)
        msg = json.loads(data.decode('utf-8'))
        return msg
