# raft-python

A simple implementation of in memory raft distributed database

### Raft algorithm

- Designed based on understandibility
- Works on a Replicated state machine
  - numerous copies of a state machine
  - Uses a replicated log
    - Allow playback of logs
    - Need the logs to be correct
- Failure cases
  - Delayed / lost messages
  - Crashing of machines

### 3 main parts

- Leader election
  - Select a leader
  - If crash create a new leader
- Log replication
  - Leader add command from client --> add to log
  - Leader replicates log to other servers
    - Overwrite inconsistencies
- Safety
  - Keep log consistent
  - Only server with up to date logs can become leader

### Server state

Start --> Follower -(no heartbeat)-> candidate -(win election)-> Leader

**When a leader, candidate discovers a higher term it returns to a follower**

- Follower
  - Expect regular hearbeat
- Candidate
  - Issues RequestVote RPC call to get elected as leader
- Leader
  - Issues AppendEntries RPC call
  - Replicates its log
  - Heartbeats to maintain leadership

Note: Communication is done via RPC

### Concepts

#### Terms

Terms --> Some designated duration of time --> contains election & normal log operation

- Terms are used to identify obsolete information
- No global view of term
- Different server have different concepts of their current term
- If machine sees other term with higher number --> ite becomes "stupid" and converts to a follower
- one leader per term
- There can be term with no leader

#### Leader election

Followers are first come first serve

#### Election correctness

- Safety
  - Allow at most one winner per term
    - Persisted on disk --> so if machine crashes after voting it knows it voted already
  - Requires majority (note if tie its not possible)
- Liveness
  - Choose election timeout randomly in [T,2T] where T is some value (e.g. 150-300 ms)
  - T should be >> broadcast time
    - If not will keep getting stuck in leader election & make not progress

#### Leader completness

- To ensure that the leader always holds the correct log
  - When receiving a request vote request
    - if my term > received req term --> deny
    - if my term == reqceived req term
      - if my log index > received log index --> deny
