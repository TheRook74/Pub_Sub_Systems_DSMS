# Part 1 — RAFT Consensus Algorithm
### Owner: Aditya
### Files: `broker/raft.py` · `common/protocol.py`

---

## What This Part Is Responsible For

This is the **brain** of the entire distributed system.
Without this part, the system has no way to agree on a single truth when multiple broker nodes exist.

RAFT answers one question:
> "If 3 broker nodes all receive write requests, how do we ensure all 3 store the **same data in the same order**, even if one node crashes?"

This part implements the complete RAFT consensus algorithm — leader election, log replication, and fault detection — plus the wire protocol used for all network communication.

---

## How It Fits Into the Whole System

```
Publisher  ──►  Broker (Part 2)  ──►  [ RAFT — THIS PART ]  ──►  LogStore (Part 2)
                                             │
                                    talks to peer brokers
                                    via protocol.py messages
```

- **Part 2 (Broker/Storage)** calls `raft.propose()` when a write arrives and calls `raft.is_leader()` to decide whether to accept or redirect a request.
- **Part 3 (Publisher/Subscriber)** is completely unaware of RAFT. It just sends messages to a broker and gets ACK or NACK back.
- RAFT internally uses `protocol.py` to encode and send messages to peer brokers.

---

## File 1: `common/protocol.py`

### Purpose
Defines every message that flows over TCP — both between clients and brokers, and between brokers internally for RAFT.

### Message Categories

**Client ↔ Broker (used by Part 2 and Part 3):**
| Message | Direction | Meaning |
|---|---|---|
| `CREATE_TOPIC <topic>` | client → broker | Create a new topic |
| `METRIC <topic> <data>` | client → broker | Publish a metric |
| `ACK` | broker → client | Request committed successfully |
| `NACK <port>` | broker → client | "I'm not the leader, try port X" |
| `SUBSCRIBE <topic> <offset>` | client → broker | Fetch new log entries |
| `DATA <new_offset>` | broker → client | Header before raw log bytes |

**RAFT Internal (broker ↔ broker, used only by Part 1):**
| Message | Meaning |
|---|---|
| `VOTE_REQUEST {json}` | Candidate asks peers for a vote |
| `VOTE_GRANTED {json}` | Peer grants vote |
| `VOTE_DENIED {json}` | Peer refuses vote |
| `REPLICATE {json}` | Leader sends log entries (also acts as heartbeat when entries=[]) |
| `REPLICATE_ACK {json}` | Follower confirms it stored the entries |

### Key Function: `recv_line(sock)`
Reads one byte at a time until `\n`. This is how the system reads a complete message without knowing its length in advance and without consuming bytes from the next message.

---

## File 2: `broker/raft.py`

### The Three States

Every broker node is always in exactly one state:

```
FOLLOWER ──(timeout)──► CANDIDATE ──(majority votes)──► LEADER
    ▲                        │                              │
    └────────────────────────┴──────(higher term seen)──────┘
```

- **FOLLOWER**: passive. Receives heartbeats from the leader. If no heartbeat arrives within `ELECTION_TIMEOUT` (7.5–15 seconds), assumes the leader is dead and becomes a CANDIDATE.
- **CANDIDATE**: trying to win an election. Sends `VOTE_REQUEST` to all peers and collects votes.
- **LEADER**: the only node that accepts write requests. Sends heartbeats every 0.75 seconds.

### The RAFT Log vs The Disk Log

There are **two separate logs** — this is the most important concept to understand:

```
self.raft_log  (in memory, list of dicts)
    │
    │  only AFTER majority confirms (commit_index advances)
    ▼
LogStore files on disk  (what subscribers actually read)
```

An entry only reaches disk after a **majority of nodes** (at least 2 out of 3) have stored it in their in-memory RAFT log and sent ACK back to the leader. This guarantees no data is lost even if the leader crashes immediately after committing.

### Class Walkthrough

```python
class RaftNode:
    def __init__(self, node_id, log_store, send_to_peer_fn):
```
- `node_id`: which broker this is (0, 1, or 2)
- `log_store`: the LogStore object from Part 2 — RAFT calls `log_store.append()` when applying committed entries
- `send_to_peer_fn`: a function provided by Part 2 (broker.py) that opens a TCP connection to another broker

### Key State Variables

| Variable | Meaning |
|---|---|
| `current_term` | Monotonically increasing election round number |
| `voted_for` | Which candidate this node voted for in current term |
| `raft_log` | In-memory list of all proposed entries |
| `commit_index` | Highest entry index confirmed by majority |
| `last_applied` | Highest entry index written to disk |
| `next_index[peer]` | What index to send next to each follower |
| `match_index[peer]` | Highest index confirmed stored on each follower |

### `propose(entry_type, topic, data)` — How a Write Enters RAFT

Called by `broker.py` (Part 2) when a publisher sends CREATE_TOPIC or METRIC:

```
1. Append new entry to self.raft_log  (in memory only)
2. Embed current timestamp in the entry  ← ensures all nodes write same timestamp to disk
3. Call _queue_replicate_all()  → sends REPLICATE to all followers immediately
4. Return the new log index to broker.py
5. broker.py then calls wait_for_commit(index) and blocks until majority confirms
```

### `handle_replicate(...)` — Follower Receives Entries from Leader

This is the most complex function. When a REPLICATE message arrives:

```
1. Verify message is from a valid leader (term check)
2. Reset election timer (leader is alive)
3. Consistency check: does our log agree with prev_log_index/prev_log_term?
   → If not: truncate our log and send NACK so leader backs up
4. Truncate stale tail (entries from a dead leader that were never committed)
5. Append the new entries
6. Advance commit_index to match the leader's
7. Send REPLICATE_ACK back
```

### `handle_vote_request(...)` — Should We Vote?

Grant vote ONLY if all three are true:
1. Candidate's term ≥ our term
2. We haven't already voted for someone else this term
3. Candidate's log is at least as long/fresh as ours (prevents electing a stale node)

### `_try_advance_commit()` — Can We Commit More?

Called after each REPLICATE_ACK arrives. Counts how many nodes have each entry. If count > cluster_size/2 (majority), that entry is committed. **Important RAFT safety rule:** only entries from the **current term** are directly committed — older-term entries are committed implicitly when a current-term entry gets committed.

### `_become_leader()` — The No-Op Fix

When elected, the leader immediately appends a **NO_OP** entry. This is from RAFT paper Section 5.4.2. Without it:
- The new leader's `commit_index` stays stale
- Entries from the previous leader accumulate and get applied in a big batch
- This causes followers' disk files to diverge in byte layout

With the NO_OP:
- `commit_index` advances immediately on all nodes
- Each entry is applied one-by-one, keeping disk files byte-identical
- Byte offsets used by the subscriber remain valid across broker switches

### Background Threads

| Thread | Role |
|---|---|
| `raft-election-timer` | Checks every 100ms if election timeout fired |
| `raft-heartbeat` | Sends REPLICATE to followers every 0.75s (leader only) |
| `raft-apply` | Watches `commit_index`; writes newly committed entries to disk |
| `raft-sender` | Drains the outgoing message queue; sends without holding the lock |

---

## How to Demo This Part Independently

You can test RAFT without any publisher or subscriber.

**Step 1 — Start all 3 brokers (3 terminals):**
```bash
cd dsms
python broker/broker.py --id 0
python broker/broker.py --id 1
python broker/broker.py --id 2
```

**Step 2 — Observe election:**
Within 7.5–15 seconds, one node wins an election:
```
[RAFT 1] Starting ELECTION | term=1
[RAFT 1] Vote from Node 2 | total=2/3
[RAFT 1] *** BECAME LEADER | term=1 ***
[RAFT 1] Appended NO_OP at index 0 (term=1)
```

**Step 3 — Test fault tolerance: kill the leader**
```
Ctrl+C on the leader's terminal
```
Within 7.5–15 seconds, the remaining two nodes elect a new leader:
```
[RAFT 2] Starting ELECTION | term=2
[RAFT 2] *** BECAME LEADER | term=2 ***
```

**Step 4 — Restart the dead broker**
```bash
python broker/broker.py --id 0
```
It catches up immediately:
```
[RAFT 0] Stepping down to FOLLOWER | term 0 → 2
[RAFT 0] FOLLOWER commit_index advanced -1 → 5 (leader=2)
```

**Step 5 — Manually inject a write to verify replication (Python REPL):**
```python
import socket
sock = socket.create_connection(("127.0.0.1", 5001))  # port of current leader
sock.sendall(b"CREATE_TOPIC test.server1\n")
print(sock.recv(100))  # should print b'ACK\n'
sock.close()
```
Then check that `broker_data/node0/`, `node1/`, `node2/` all have `test_server1.log`.

---

## Integration Points

| What Part 2 calls on this part | When |
|---|---|
| `raft.is_leader()` | Before accepting any client write |
| `raft.get_leader_port()` | To build the NACK redirect response |
| `raft.propose(type, topic, data)` | When a CREATE_TOPIC or METRIC arrives |
| `raft.wait_for_commit(index)` | After propose(), to know when to send ACK |
| `raft.handle_vote_request(...)` | When a VOTE_REQUEST TCP message arrives |
| `raft.handle_replicate(...)` | When a REPLICATE TCP message arrives |
| `raft.handle_replicate_ack(...)` | When a REPLICATE_ACK TCP message arrives |

What this part calls on Part 2:
- `log_store.create_topic(topic)` — called from `_apply_entry()` when a CREATE_TOPIC is committed
- `log_store.append(topic, data, ts)` — called from `_apply_entry()` when a METRIC is committed
- `send_to_peer_fn(peer_id, message)` — the TCP sender injected by broker.py
