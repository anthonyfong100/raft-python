from raft_python.socket_wrapper import SocketWrapper
from raft_python.configs import BROADCAST_ADDR


class KVStore:
    def __init__(self, socket_wrapper: SocketWrapper, id, others):
        self.socket: SocketWrapper = socket_wrapper
        self.id = id
        self.others = others

        self.send_hello()

    def send_hello(self):
        print("Replica %s starting up" % self.id, flush=True)
        hello = {"src": self.id, "dst": BROADCAST_ADDR,
                 "leader": BROADCAST_ADDR, "type": "hello"}
        self.socket.send(hello)
        print("Sent hello message: %s" % hello, flush=True)

    def run(self):
        while True:
            msg = self.socket.receive()
            print("Received message '%s'" % (msg,), flush=True)
