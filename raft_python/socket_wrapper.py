import logging
import socket
import json
from raft_python.configs import BUFFER_SIZE, LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class SocketWrapper:
    def __init__(self, port: int):
        self.port = port
        self.socket = self._setup_socket()

    def _setup_socket(self):
        _socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _socket.bind(('localhost', 0))
        return _socket

    def send(self, message_serialized: dict):
        try:
            self.socket.sendto(json.dumps(message_serialized).encode(
                'utf-8'), ('localhost', self.port))
        except:
            # logger.critical(
            #     f"error sending message: {message_serialized['src']} {message_serialized['dst']}")
            pass

    def receive(self, buff_size=BUFFER_SIZE) -> dict:
        data, _ = self.socket.recvfrom(buff_size)
        msg = json.loads(data.decode('utf-8'))
        return msg

    def close(self):
        self.socket.close()
