from typing import Union
from raft_python.configs import BROADCAST_ADDR
from enum import Enum


class MessageTypes(Enum):
    GET = "get"
    PUT = "put"
    OK = "ok"
    FAIL = "fail"
    REDIRECT = "redirect"


class BaseMessage:
    def __init__(self,
                 src: str,
                 dst: str,
                 type: MessageTypes,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        self.src: str = src
        self.dst: str = dst
        self.leader: str = leader
        self.type: MessageTypes = type
        self.MID: str = MID

    def serialize(self):
        # need to make a copy as it returns the reference
        serialize_dict: dict = vars(self).copy()
        serialize_dict["type"] = self.type.value
        return serialize_dict

# Request message wrappers


class GetMessageRequest(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.GET, MID, leader)
        self.key = key


class PutMessageRequest(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 val: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.PUT, MID, leader)
        self.key = key
        self.val = val


# Reponse message wrappers
class GetMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 val: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, MID, leader)
        self.val = val


class PutMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, MID, leader)


class MessageFail(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.FAIL, MID, leader)


class MessageRedirect(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ADDR):
        super().__init__(src, dst, MessageTypes.REDIRECT, MID, leader)


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


ReqMessageType = Union[GetMessageRequest, PutMessageRequest]
