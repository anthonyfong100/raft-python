from typing import Union
from raft_python.configs import BROADCAST_ALL_ADDR
from enum import Enum


class MessageTypes(Enum):
    GET = "get"
    PUT = "put"
    OK = "ok"
    FAIL = "fail"
    REDIRECT = "redirect"
    HELLO = "hello"
    REQUEST_VOTE = "request_vote"
    REQUEST_VOTE_RESPONSE = "request_vote_response"


class BaseMessage:
    def __init__(self,
                 src: str,
                 dst: str,
                 type: MessageTypes,
                 leader: str = BROADCAST_ALL_ADDR):
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

# Client Request message wrappers


class HelloMessage(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.HELLO, leader)


class GetMessageRequest(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 leader: str = BROADCAST_ALL_ADDR):
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
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.PUT, leader)
        self.key = key
        self.val = val
        self.MID = MID


# Client Reponse message wrappers
class GetMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 val: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.val = val
        self.MID = MID


class PutMessageResponseOk(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.MID = MID


class MessageFail(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.FAIL, leader)
        self.MID = MID


class MessageRedirect(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.REDIRECT, leader)
        self.MID = MID


# Raft algorithm request wrappers
class RequestVote(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 term_number: int,
                 candidate_id: str,
                 last_log_index: int,
                 last_log_term_number: int,
                 leader: str = BROADCAST_ALL_ADDR) -> None:
        super().__init__(src, dst, MessageTypes.REQUEST_VOTE, leader)
        self.term_number = term_number
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term_number = last_log_term_number


class RequestVoteResponse(BaseMessage):
    def __init__(self,
                 src: str,
                 dst: str,
                 term_number: int,
                 vote_granted: bool,
                 leader: str = BROADCAST_ALL_ADDR) -> None:
        super().__init__(src, dst, MessageTypes.REQUEST_VOTE_RESPONSE, leader)
        self.term_number = term_number
        self.vote_granted = vote_granted


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
    elif payload.get("type", None) == MessageTypes.REQUEST_VOTE.value:
        return RequestVote(
            payload["src"],
            payload["dst"],
            int(payload["term_number"]),
            payload["candidate_id"],
            payload["last_log_index"],
            payload["last_log_term_number"],
            payload["leader"]
        )
    elif payload.get("type", None) == MessageTypes.REQUEST_VOTE_RESPONSE.value:
        return RequestVoteResponse(
            int(payload["term_number"]),
            payload["vote_granted"],
        )
    raise ValueError(
        f"Received payload is of unknown type\n Payload:{payload}")


InternalMessageType = Union[RequestVote, RequestVoteResponse]
ClientMessageType = Union[GetMessageRequest, PutMessageRequest]
ReqMessageType = Union[HelloMessage, GetMessageRequest,
                       PutMessageRequest, RequestVote, RequestVoteResponse]
