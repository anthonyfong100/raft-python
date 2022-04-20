from typing import Union
from raft_python.configs import BROADCAST_ADDR
from enum import Enum


class MessageTypes(Enum):
    GET = "get"
    PUT = "put"
    OK = "ok"
    FAIL = "fail"
    REDIRECT = "redirect"
    HELLO = "hello"


class BaseMessage:
    def __init__(self,
                 src: str,
                 dst: str,
                 type: MessageTypes,
                 leader: str = BROADCAST_ADDR):
        self.src: str = src
        self.dst: str = dst
        self.leader: str = leader
        self.type: MessageTypes = type

    def serialize(self):
        # need to make a copy as it returns the reference
        serialize_dict: dict = vars(self).copy()
        serialize_dict["type"] = self.type.value
        return serialize_dict

    def __eq__(self, __o: object) -> bool:
        return vars(self) == vars(__o)

# Request message wrappers


class HelloMessage(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.HELLO, leader)


class GetMessageRequest(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.GET, leader)
        self.key = key
        self.MID = MID


class PutMessageRequest(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 val: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.PUT, leader)
        self.key = key
        self.val = val
        self.MID = MID


# Reponse message wrappers
class GetMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 val: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.val = val
        self.MID = MID


class PutMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.MID = MID


class MessageFail(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.FAIL, leader)
        self.MID = MID


class MessageRedirect(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.REDIRECT, leader)
        self.MID = MID


def get_message_from_payload(payload: dict) -> Union[GetMessageRequest, PutMessageRequest]:
    if payload.get("type", None) == MessageTypes.GET.value:
        return GetMessageRequest(
            payload["src"],
            payload["dst"],
            payload["MID"],
            payload["key"],
            payload["leader"])
    elif payload.get("type", None) == MessageTypes.PUT.value:
        return PutMessageRequest(
            payload["src"],
            payload["dst"],
            payload["MID"],
            payload["key"],
            payload["value"],
            payload["leader"])


ReqMessageType = Union[HelloMessage, GetMessageRequest, PutMessageRequest]
