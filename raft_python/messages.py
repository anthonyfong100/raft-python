from typing import Union, List
from raft_python.configs import BROADCAST_ALL_ADDR
from raft_python.commands import ALL_COMMANDS, deserialize_command
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
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"


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
    name = "HelloMessage"

    def __init__(self,
                 src: str,
                 dst: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.HELLO, leader)


class GetMessageRequest(BaseMessage):
    name = "GetMessageRequest"

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
    name = "PutMessageRequest"

    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 key: str,
                 value: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.PUT, leader)
        self.key = key
        self.value = value
        self.MID = MID


# Client Reponse message wrappers
class GetMessageResponseOk(BaseMessage):
    name = "GetMessageResponseOk"

    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 value: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.value = value
        self.MID = MID


class PutMessageResponseOk(BaseMessage):
    name = "PutMessageResponseOk"

    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(
            src, dst, MessageTypes.OK, leader)
        self.MID = MID


class MessageFail(BaseMessage):
    name = "MessageFail"

    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.FAIL, leader)
        self.MID = MID


class MessageRedirect(BaseMessage):
    name = "MessageRedirect"

    def __init__(self,
                 src: str,
                 dst: str,
                 MID: str,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.REDIRECT, leader)
        self.MID = MID


# Raft algorithm request wrappers
class RequestVote(BaseMessage):
    name = "RequestVote"

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
    name = "RequestVoteResponse"

    def __init__(self,
                 src: str,
                 dst: str,
                 term_number: int,
                 vote_granted: bool,
                 leader: str = BROADCAST_ALL_ADDR) -> None:
        super().__init__(src, dst, MessageTypes.REQUEST_VOTE_RESPONSE, leader)
        self.term_number = term_number
        self.vote_granted = vote_granted


class AppendEntriesReq(BaseMessage):
    name = "AppendEntriesReq"

    def __init__(self,
                 src: str,
                 dst: str,
                 term_number: int,
                 leader_id: str,
                 prev_log_index: int,
                 prev_log_term_number: int,
                 entries: List[ALL_COMMANDS],
                 leader_commit_index: int,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.APPEND_ENTRIES, leader)
        self.term_number = term_number
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term_number = prev_log_term_number
        self.entries = entries
        self.leader_commit_index = leader_commit_index

    def serialize(self):
        dict = super().serialize()
        dict["entries"] = [entry.serialize() for entry in dict["entries"]]
        return dict


class AppendEntriesResponse(BaseMessage):
    name = "AppendEntriesResponse"

    def __init__(self,
                 src: str,
                 dst: str,
                 term_number: int,
                 match_index: int,
                 success: bool,
                 leader: str = BROADCAST_ALL_ADDR):
        super().__init__(src, dst, MessageTypes.APPEND_ENTRIES_RESPONSE, leader)
        self.term_number = term_number
        self.match_index = match_index
        self.success = success


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
            payload["src"],
            payload["dst"],
            int(payload["term_number"]),
            payload["vote_granted"],
            payload["leader"]
        )
    elif payload.get("type", None) == MessageTypes.APPEND_ENTRIES.value:
        entries = [deserialize_command(serialized_entry)
                   for serialized_entry in payload["entries"]]

        return AppendEntriesReq(
            payload["src"],
            payload["dst"],
            int(payload["term_number"]),
            payload["leader_id"],
            payload["prev_log_index"],
            payload["prev_log_term_number"],
            entries,
            payload["leader_commit_index"],
            payload["leader"],
        )
    elif payload.get("type", None) == MessageTypes.APPEND_ENTRIES_RESPONSE.value:
        return AppendEntriesResponse(
            payload["src"],
            payload["dst"],
            int(payload["term_number"]),
            payload["match_index"],
            payload["success"],
            payload["leader"],
        )
    raise ValueError(
        f"Received payload is of unknown type\n Payload:{payload}")


InternalMessageType = Union[RequestVote, RequestVoteResponse,
                            AppendEntriesReq, AppendEntriesResponse]
ClientMessageType = Union[GetMessageRequest, PutMessageRequest]
IncomingMessageType = Union[InternalMessageType, ClientMessageType]


def is_internal_message(msg: IncomingMessageType):
    return type(msg) == RequestVote or type(msg) == RequestVoteResponse or \
        type(msg) == AppendEntriesReq or type(msg) == AppendEntriesResponse


def is_client_message(msg: IncomingMessageType):
    return type(msg) == GetMessageRequest or type(msg) == PutMessageRequest
