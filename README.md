# DSMS — Distributed Server Monitoring System
### CS60002 Distributed Systems | Group 8 | Spring 2026

A **pub-sub system** built from scratch in Python that monitors OS processes
and distributes real-time CPU/memory metrics across a fault-tolerant broker
cluster using the **RAFT consensus algorithm**.

---

## Table of Contents
1. [What This System Does](#1-what-this-system-does)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Deep-Dive](#3-component-deep-dive)
4. [Data Flow (Step by Step)](#4-data-flow-step-by-step)
5. [Message Protocol](#5-message-protocol)
6. [RAFT Consensus — How It Works Here](#6-raft-consensus--how-it-works-here)
7. [Directory Structure](#7-directory-structure)
8. [Setup and Running](#8-setup-and-running)
9. [HTTP API Reference](#9-http-api-reference)
10. [Fault Tolerance Scenarios](#10-fault-tolerance-scenarios)
11. [Configuration Reference](#11-configuration-reference)

---

## 1. What This System Does

DSMS is a **distributed monitoring system** built on a **publish-subscribe
(pub-sub) architecture**.

- **Publishers** (`DSMS_PUB`) run on server machines.  They read a config
  file (`apps.json`) to know which OS processes to watch, then periodically
  collect CPU and memory usage using `psutil` and send it to the broker
  cluster.

- **Brokers** form a **RAFT cluster** (3 nodes by default).  They store all
  incoming metrics in append-only log files — one file per topic.  RAFT
  ensures every broker has the exact same data in the exact same order, even
  if one broker crashes and restarts.

- **Subscribers** (`DSMS_SUB`) use a **pull model**: they ask the broker for
  new data periodically using a byte offset.  `DSMS_SUB` also exposes an
  HTTP REST API so dashboards and alert tools can query metrics without
  knowing anything about the broker cluster.

---

## 2. Architecture Overview

```
  ┌──────────────┐        METRIC / CREATE_TOPIC         ┌─────────────────────────────┐
  │  DSMS_PUB    │ ────────────────────────────────────► │  Broker Cluster (RAFT)      │
  │  (publisher) │ ◄──────────────── ACK / NACK ─────── │                             │
  └──────────────┘                                       │  ┌─────┐  ┌─────┐  ┌─────┐ │
                                                         │  │  0  │◄►│  1  │◄►│  2  │ │
  ┌──────────────┐        SUBSCRIBE <topic> <offset>     │  │LEAD │  │FOLL │  │FOLL │ │
  │  DSMS_SUB    │ ────────────────────────────────────► │  └─────┘  └─────┘  └─────┘ │
  │ (subscriber) │ ◄──────────────── DATA <bytes> ─────  │                             │
  └──────┬───────┘                                       └─────────────────────────────┘
         │  HTTP REST API
         │
  ┌──────▼───────┐
  │  Clients:    │
  │  dashboards, │
  │  curl, etc.  │
  └──────────────┘
```

### Key Design Choices

| Concern | Choice | Reason |
|---|---|---|
| Subscriber model | **Pull** (subscriber polls broker) | Subscriber controls its own pace; no overload |
| Log format | **Append-only, offset-based** | Efficient; subscriber never re-reads old data |
| Consensus | **RAFT** | Single leader guarantees ordering; handles crashes |
| Storage | Per-topic log FILES on disk | Survives broker crashes; simple to implement |
| Aggregation | Done in DSMS_SUB, NOT the broker | Broker stays simple; subscriber merges prefix topics |

---

## 3. Component Deep-Dive

### 3.1 DSMS_PUB (`publisher/dsms_pub.py`)

**Responsibilities:**
- Reads `apps.json` for the list of apps and a server ID.
- Builds topics: `{app_name}.{server_id}` → e.g. `python.server1`.
- Sends `CREATE_TOPIC` for each topic at startup (retries until success).
- Every `PUBLISH_INTERVAL` seconds: collects CPU/memory via `psutil` and
  sends `METRIC` to the leader broker.
- Handles `NACK` (wrong broker) by caching and redirecting to the real leader.
- If the leader crashes, automatically discovers the new leader.

### 3.2 Broker Cluster (`broker/broker.py`)

Each broker is a TCP server that:
- Accepts connections from publishers, subscribers, AND peer brokers.
- Routes `METRIC` / `CREATE_TOPIC` through RAFT before responding `ACK`.
- Handles `SUBSCRIBE` by reading from the local `LogStore` at the given offset.
- Sends `NACK <leader_port>` to clients that contacted a follower.

### 3.3 RAFT Node (`broker/raft.py`)

The consensus engine.  Key behaviors:
- **Election timer**: if a follower doesn't receive a `REPLICATE` (heartbeat)
  within a random 2–4 second window, it starts an election.
- **Heartbeat loop**: the leader broadcasts `REPLICATE` every 0.75 seconds.
- **Log replication**: when the leader appends an entry, it immediately sends
  `REPLICATE` to all followers.  Once a majority responds with
  `REPLICATE_ACK`, the entry is committed.
- **Apply thread**: watches `commit_index` and writes committed entries to the
  `LogStore` on disk.

### 3.4 LogStore (`broker/log_store.py`)

One log FILE per topic, stored in `broker_data/nodeN/` on each broker.
Each line is a JSON object:
```json
{"ts": "2026-04-05 10:30:01", "data": "cpu=70.5 memory=45.2"}
```
Subscribers read using a **byte offset**: the broker seeks to that byte
position and returns only new bytes.  This makes polling very efficient.

### 3.5 DSMS_SUB (`subscriber/dsms_sub.py`)

Two concurrent parts:
1. **Broker Poller** (one background thread per subscribed topic): sends
   `SUBSCRIBE <topic> <offset>` every `POLL_INTERVAL` seconds, stores results.
2. **Flask HTTP Server**: exposes REST endpoints so any client can query data.

### 3.6 LogManager (`subscriber/log_manager.py`)

In-memory cache of all fetched log entries.  Also handles **aggregation**:
querying `app1` returns entries from all topics matching `app1.*`.

---

## 4. Data Flow (Step by Step)

```
Step 1 — Publisher Startup
   DSMS_PUB reads apps.json
   → topics = ["python.server1", "chrome.server1"]
   → sends CREATE_TOPIC python.server1 to any broker
   → broker leader adds to RAFT log → replicates to followers → commits
   → ALL 3 brokers now have the topic file on disk
   → broker sends ACK to publisher

Step 2 — Publisher Metric Loop (every 5 seconds)
   psutil reads cpu=70.5 memory=45.2 for "python" process
   → DSMS_PUB sends: METRIC python.server1 cpu=70.5 memory=45.2
   → Broker leader appends to RAFT log
   → Broadcasts REPLICATE to followers
   → Followers store entry, send REPLICATE_ACK
   → Once majority ACK: commit_index advances
   → Apply thread writes to python_server1.log on disk
   → Broker sends ACK to publisher

Step 3 — Subscriber Polls (every 3 seconds)
   DSMS_SUB sends: SUBSCRIBE python.server1 0   (first time, offset=0)
   → Broker reads from byte 0 of python_server1.log
   → Sends: DATA 185\n{"ts":"2026-04-05 10:30:01","data":"cpu=70.5 memory=45.2"}\n
   → DSMS_SUB stores entries, remembers offset=185

   Next poll: SUBSCRIBE python.server1 185
   → Broker reads from byte 185 (only NEW entries since last poll)

Step 4 — Client Queries HTTP API
   curl "http://localhost:8080/logs?topic=python.server1"
   → DSMS_SUB returns cached JSON entries immediately (no broker contact)
```

---

## 5. Message Protocol

All messages are **plain-text, newline-terminated** TCP lines.  This makes
them easy to debug with tools like `nc` (netcat) or `telnet`.

### Client ↔ Broker

| Message | Direction | Format |
|---|---|---|
| `CREATE_TOPIC` | Publisher → Broker | `CREATE_TOPIC app1.server1\n` |
| `METRIC` | Publisher → Broker | `METRIC app1.server1 cpu=70.5 memory=45.2\n` |
| `ACK` | Broker → Client | `ACK\n` |
| `NACK` | Broker → Client | `NACK 5001\n` (redirect to leader on port 5001) |
| `SUBSCRIBE` | Subscriber → Broker | `SUBSCRIBE app1.server1 0\n` |
| `DATA` | Broker → Subscriber | `DATA 185\n` followed by raw log bytes |

### RAFT Internal (Broker ↔ Broker)

| Message | Format |
|---|---|
| `VOTE_REQUEST` | `VOTE_REQUEST {"term":2,"candidate_id":1,...}\n` |
| `VOTE_GRANTED` | `VOTE_GRANTED {"term":2,"voter_id":0}\n` |
| `VOTE_DENIED` | `VOTE_DENIED {"term":2}\n` |
| `REPLICATE` | `REPLICATE {"term":2,"leader_id":1,"entries":[...],...}\n` |
| `REPLICATE_ACK` | `REPLICATE_ACK {"term":2,"follower_id":0,"match_index":5,"success":true}\n` |

---

## 6. RAFT Consensus — How It Works Here

RAFT ensures that all broker nodes store exactly the same log in the same
order, so every broker can serve subscribers with consistent data.

### 6.1 Leader Election

```
Initially: all 3 nodes are FOLLOWERS.

Each follower has a random election timeout (2–4 seconds).
The node whose timer expires first:
  1. Increments its term (e.g. term 0 → 1)
  2. Votes for itself
  3. Sends VOTE_REQUEST to the other 2 nodes

Each peer grants the vote IF:
  - Candidate's term >= its own term
  - It hasn't voted for someone else in this term
  - Candidate's log is at least as up-to-date

If the candidate gets 2+ votes (majority of 3): it becomes LEADER.
The new leader immediately sends REPLICATE (heartbeat) to suppress
any other node from starting its own election.
```

### 6.2 Log Replication

```
Client sends METRIC to leader.
Leader appends entry to its in-memory RAFT log (index N, term T).
Leader broadcasts REPLICATE to both followers.
Each follower:
  - Checks that its log is consistent up to prev_log_index
  - Appends the new entry
  - Sends REPLICATE_ACK (success=True, match_index=N)
Leader receives REPLICATE_ACK from follower 1 (now 2 of 3 nodes have it).
Majority reached! Leader sets commit_index = N.
Apply thread sees commit_index > last_applied → writes to disk.
Leader sends ACK to client.
```

### 6.3 Fault Tolerance

| Scenario | What Happens |
|---|---|
| Leader crashes | Followers time out → election → new leader elected |
| Follower crashes | Other 2 nodes form quorum; system keeps working |
| Publisher contacts dead leader | Gets connection error → scans other brokers |
| Publisher contacts follower | Gets NACK with leader port → retries leader |
| Subscriber contacts dead broker | Gets error → tries next broker in list |

---

## 7. Directory Structure

```
dsms/
│
├── common/                    # Shared code used by all components
│   ├── config.py              # Cluster topology, timeouts, intervals
│   └── protocol.py            # Message encode/decode, socket helpers
│
├── broker/                    # Broker node code
│   ├── log_store.py           # Append-only disk log per topic
│   ├── raft.py                # RAFT consensus algorithm
│   └── broker.py              # TCP server + message routing
│
├── publisher/                 # Publisher (DSMS_PUB) code
│   ├── metrics.py             # psutil CPU/memory collection
│   ├── dsms_pub.py            # Main publisher loop
│   └── apps.json              # Configure which apps to monitor
│
├── subscriber/                # Subscriber (DSMS_SUB) code
│   ├── log_manager.py         # In-memory log cache + aggregation
│   └── dsms_sub.py            # Broker poller + Flask HTTP API
│
├── broker_data/               # Created at runtime — one dir per broker
│   ├── node0/                 # Broker 0's log files
│   ├── node1/                 # Broker 1's log files
│   └── node2/                 # Broker 2's log files
│
├── start_cluster.py           # Helper to start all 3 brokers at once
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## 8. Setup and Running

### 8.1 Install Dependencies

```bash
cd "Distr. Sys/dsms"
pip install -r requirements.txt
```

### 8.2 Edit `apps.json` (Optional)

Open `publisher/apps.json` and change the app names to processes actually
running on your machine.  To list all running process names:

```python
from publisher.metrics import list_running_processes
for pid, name in list_running_processes():
    print(pid, name)
```

Default `apps.json`:
```json
{
    "server_id": "server1",
    "apps": ["python", "chrome", "code"]
}
```

> **Tip:** Use partial names.  `"chrome"` will match `"chrome.exe"` on Windows.
> If a process is not found, the publisher falls back to system-wide metrics.

### 8.3 Start the Broker Cluster

Open **3 separate terminal windows** and run one command in each:

**Terminal 1 (Broker 0 — will likely become leader):**
```bash
cd "Distr. Sys/dsms"
python broker/broker.py --id 0
```

**Terminal 2 (Broker 1):**
```bash
cd "Distr. Sys/dsms"
python broker/broker.py --id 1
```

**Terminal 3 (Broker 2):**
```bash
cd "Distr. Sys/dsms"
python broker/broker.py --id 2
```

You should see output like:
```
[RAFT 0] Started as FOLLOWER | term=0 | peers=[1, 2]
[RAFT 0] Starting ELECTION | term=1
[RAFT 0] *** BECAME LEADER | term=1 ***
```

### 8.4 Start the Publisher

**Terminal 4:**
```bash
cd "Distr. Sys/dsms"
python publisher/dsms_pub.py
```

Output:
```
[PUB] Topics: ['python.server1', 'chrome.server1', 'code.server1']
[PUB] Creating topic 'python.server1' (attempt 1/10)
[PUB] Topic 'python.server1' created successfully.
[PUB] Sending METRIC | topic=python.server1 | cpu=12.3 memory=1.45
```

### 8.5 Start the Subscriber

**Terminal 5:**
```bash
cd "Distr. Sys/dsms"
python subscriber/dsms_sub.py
```

Output:
```
[SUB] HTTP API running at http://127.0.0.1:8080
```

### 8.6 Subscribe and Read Metrics

**In any terminal (or browser):**

Subscribe to a topic:
```bash
curl -X POST "http://localhost:8080/subscribe?topic=python.server1"
```

Wait ~3 seconds for the first poll, then read logs:
```bash
curl "http://localhost:8080/logs?topic=python.server1"
```

Subscribe to ALL server1 topics at once (aggregated view):
```bash
curl -X POST "http://localhost:8080/subscribe?topic=chrome.server1"
curl "http://localhost:8080/logs?topic=server1"
```

Check subscription status:
```bash
curl "http://localhost:8080/status"
```

---

## 9. HTTP API Reference

Base URL: `http://localhost:8080`

### `POST /subscribe?topic=<topic>`
Start collecting data for a topic.  Spawns a background poller thread.
```
Response: {"status": "subscribed", "topic": "python.server1"}
```

### `DELETE /subscribe?topic=<topic>`
Stop collecting data for a topic.
```
Response: {"status": "unsubscribed", "topic": "python.server1"}
```

### `GET /logs?topic=<topic>`
Return all cached log entries.  Supports prefix aggregation.
```
GET /logs?topic=python.server1   → entries for that leaf topic only
GET /logs?topic=server1          → entries from ALL *.server1 topics merged
GET /logs?topic=python           → entries from ALL python.* topics merged
```
Response:
```json
{
  "topic": "python.server1",
  "entry_count": 5,
  "entries": [
    {"ts": "2026-04-05 10:30:01", "data": "cpu=12.3 memory=1.45", "topic": "python.server1"},
    {"ts": "2026-04-05 10:30:06", "data": "cpu=13.1 memory=1.47", "topic": "python.server1"}
  ]
}
```

### `GET /status`
Show all active subscriptions and their cached entry counts.
```json
{
  "subscriptions": {
    "python.server1": {"entry_count": 5, "byte_offset": 472}
  },
  "leader_port": 5000
}
```

---

## 10. Fault Tolerance Scenarios

### Kill the leader broker while publisher is running:
1. Note which broker is the leader (check terminal output).
2. Close that terminal window (Ctrl+C).
3. The other two brokers detect the heartbeat timeout after ~2–4 seconds.
4. They elect a new leader.
5. The publisher gets a connection error, finds the new leader via NACK,
   and resumes publishing — **no data loss**.

### Kill a follower broker:
- The remaining 2 nodes (leader + 1 follower) still form a majority of 3.
- System continues normally.
- Restart the follower and it will catch up automatically via REPLICATE.

### Kill DSMS_SUB and restart:
- On restart, re-subscribe to your topics.
- The broker still has all the data from byte offset 0.
- DSMS_SUB will catch up from offset 0 on the first poll.

---

## 11. Configuration Reference

All settings live in `common/config.py`.

| Variable | Default | Description |
|---|---|---|
| `BROKERS` | 3 nodes on ports 5000–5002 | Cluster topology |
| `ELECTION_TIMEOUT_MIN` | 2.0 s | Minimum follower timeout |
| `ELECTION_TIMEOUT_MAX` | 4.0 s | Maximum follower timeout |
| `HEARTBEAT_INTERVAL` | 0.75 s | Leader heartbeat frequency |
| `PUBLISH_INTERVAL` | 5 s | Publisher metric collection interval |
| `POLL_INTERVAL` | 3 s | Subscriber broker polling interval |
| `MAX_RETRIES` | 10 | Max retries for topic creation |
| `SUBSCRIBER_HTTP_PORT` | 8080 | HTTP API port |

---

*CS60002 Distributed Systems — Group 8: Karthik Reddy, Priyanshu Gourav, Aditya Kumar Bharti*
