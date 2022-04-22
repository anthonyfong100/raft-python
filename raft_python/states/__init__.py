from typing import Union
from .candidate import Candidate
from .follower import Follower
from .leader import Leader

ALL_NODE_STATE = Union[Follower, Candidate, Leader]
