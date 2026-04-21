# Part 2 — Broker & Storage Layer

### Owner: Aditya Kumar Bharti

### Files: `broker/broker.py` · `broker/log_store.py` · `common/config.py` · `start_cluster.py`

---

## What This Part Is Responsible For

This part is the **infrastructure** of the system — the TCP server that all clients connect to, and the disk storage that persists all metric data.

It acts as the **glue** between the two other parts:

- It receives messages from Part 3 (Publisher/Subscriber) over TCP
- It hands write requests to Part 1 (RAFT) for consensus
- It writes committed data to disk (LogStore) which Part 3 reads back

Without this part, RAFT has no network to communicate through, and there is no storage for metric data.

---

## How It Fits Into the Whole System

```
                      ┌─────────────────────────────────┐
Publisher  ──TCP──►   │  broker.py  (THIS PART)          │
Subscriber ──TCP──►   │    ├── routes messages           │  ──► RAFT (Part 1)
Peer broker ──TCP──►  │    ├── manages TCP server        │
                      │    └── calls LogStore            │  ──► disk files
                      └─────────────────────────────────┘
```

---

## File 1: `common/config.py`

### Purpose

Single place to configure the entire cluster. Every other file imports from here. To deploy on 3 different machines, **only this file changes**.

### Key Settings

```python
BROKERS = [
    {"id": 0, "host": "10.145.2.166", "port": 5000},
    {"id": 1, "host": "10.145.9.145", "port": 5001},
    {"id": 2, "host": "10.145.44.49", "port": 5002},
]
```

Each entry describes one broker node. `id` must match what you pass to `--id` on the command line.

```python
ELECTION_TIMEOUT_MIN = 7.5   # seconds
ELECTION_TIMEOUT_MAX = 15.0  # seconds
HEARTBEAT_INTERVAL   = 0.75  # seconds
```

RAFT timing. Per the RAFT paper, election timeout must be >> heartbeat (10× rule). Current values: 7.5s minimum is exactly 10× the 0.75s heartbeat.

```python
PUBLISH_INTERVAL     = 5     # seconds  (publisher sends metrics every 5s)
POLL_INTERVAL        = 3     # seconds  (subscriber checks for new data every 3s)
SUBSCRIBER_HTTP_PORT = 8080
```

---

## File 2: `broker/log_store.py`

### Purpose

Manages append-only log files on disk — one file per topic per broker node.

### Why Append-Only?

This design is borrowed from Apache Kafka. Benefits:

1. No random writes — sequential disk writes are the fastest possible I/O
2. Crash safety — partially written entry at the end can be detected; previous entries are intact
3. Offset-based reads — subscriber just remembers a byte position and seeks directly there, no scanning

### Topic → File Mapping

```
"python.server1"  →  broker_data/node0/python_server1.log
"chrome.server1"  →  broker_data/node0/chrome_server1.log
```

Dots become underscores because `.` has special meaning in file paths.

### `create_topic(topic)` — Create a New Log File

```python
def create_topic(self, topic: str) -> bool:
    path = self._get_path(topic)
    with self._lock:
        if not os.path.exists(path):
            open(path, "a").close()   # creates empty file
            return True
    return False
```

Called by RAFT's `_apply_entry()` when a CREATE_TOPIC entry is committed. Uses a threading lock so two threads can't create the same file simultaneously.

### `append(topic, data, ts=None)` — Write a Metric Entry

```python
entry = json.dumps({"ts": ts, "data": data}) + "\n"
with open(path, "a") as f:
    f.write(entry)
```

Each entry is one JSON line. The `ts` (timestamp) is passed in from the RAFT log — it was recorded by the leader when the entry was proposed. This ensures all 3 broker nodes write the **exact same timestamp**, making their files byte-for-byte identical.

### `read(topic, byte_offset)` — Fetch New Entries

```python
def read(self, topic, byte_offset):
    with open(path, "rb") as f:
        f.seek(byte_offset)     # jump directly to where we left off
        content = f.read()      # read from there to end of file
    new_offset = byte_offset + len(content)
    return content, new_offset
```

The subscriber passes back `new_offset` on the next call. This is how the subscriber reads only new data without re-reading the whole file every time.

**Example:**

```
File contents (80 bytes total):
  Byte 0:  {"ts": "22:14:00", "data": "cpu=5.0 memory=45.0"}\n   (40 bytes)
  Byte 40: {"ts": "22:14:05", "data": "cpu=6.2 memory=45.1"}\n   (40 bytes)

First read:  offset=0  → returns 80 bytes, new_offset=80
Second read: offset=80 → returns 0 bytes  (nothing new yet)
After new write: offset=80 → returns 40 bytes, new_offset=120
```

---

## File 3: `broker/broker.py`

### Purpose

The main process. Starts a TCP server, accepts connections, reads one message per connection, routes it to the right handler.

### Class Structure

```python
class Broker:
    def __init__(self, node_id):
        self.log_store = LogStore(base_dir=f"broker_data/node{node_id}")
        self.raft = RaftNode(node_id, self.log_store, self._send_to_peer)
        # TCP server socket
        self.server_sock.bind((host, port))
        self.server_sock.listen(50)
```

Three objects are created and connected:

- `LogStore` — disk storage
- `RaftNode` — consensus (gets `_send_to_peer` as its network function)
- TCP server socket

### `serve_forever()` — The Accept Loop

```python
while True:
    conn, addr = self.server_sock.accept()
    threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True).start()
```

Each incoming connection gets its own daemon thread. This allows many publishers and subscribers to be served concurrently without blocking each other.

### `_handle_connection()` — Message Router

Reads one line from the socket, decodes the message type, routes to the right handler:

```
CREATE_TOPIC  → _handle_create_topic()
METRIC        → _handle_metric()
SUBSCRIBE     → _handle_subscribe()
VOTE_REQUEST  → raft.handle_vote_request()    ← RAFT internal
VOTE_GRANTED  → raft.handle_vote_granted()    ← RAFT internal
REPLICATE     → raft.handle_replicate()       ← RAFT internal
REPLICATE_ACK → raft.handle_replicate_ack()   ← RAFT internal
```

### `_handle_create_topic()` — Write Path for Topic Creation

```
1. Not leader?  → send NACK with leader port  (publisher will retry at leader)
2. Topic already exists?  → send ACK immediately (idempotent)
3. Otherwise:
   a. raft.propose("CREATE_TOPIC", topic)     → adds to RAFT log
   b. raft.wait_for_commit(index, timeout=6s) → blocks until majority confirms
   c. send ACK                                → publisher knows it's durable
```

### `_handle_metric()` — Write Path for Metrics

Same flow as CREATE_TOPIC. One extra check: if the topic doesn't exist on this broker, it means the topic was never created — send ERROR.

### `_handle_subscribe()` — Read Path

```
1. Not leader?  → send NACK
2. log_store.read(topic, byte_offset) → get new bytes
3. send: "DATA <new_offset>\n"        ← header line
4. send: <raw log bytes>              ← actual content
```

The subscriber reads the header to get `new_offset`, then reads all remaining bytes until connection closes.

### `_send_to_peer(peer_id, message)` — Inter-Broker TCP

```python
with socket.create_connection((peer_host, peer_port), timeout=1.0) as s:
    s.sendall(message.encode())
```

Fire-and-forget. RAFT calls this to send VOTE_REQUEST, REPLICATE etc. to other brokers. If the peer is down, the exception is silently caught — RAFT is designed to handle node failures.

---

## File 4: `start_cluster.py`

A convenience script that starts all 3 broker processes at once using `subprocess`:

```bash
python start_cluster.py
```

Useful on a single machine for testing. For multi-machine deployment, each machine runs `python broker/broker.py --id X` manually.

---

## How to Demo This Part Independently

You can test broker and storage without any publisher or subscriber running.

**Step 1 — Start brokers:**

```bash
cd dsms
python broker/broker.py --id 0   # terminal 1
python broker/broker.py --id 1   # terminal 2
python broker/broker.py --id 2   # terminal 3
```

**Step 2 — Wait for leader election, then find which node is leader from the logs:**

```
[RAFT 1] *** BECAME LEADER | term=1 ***    ← Node 1 is leader, port 5001
```

**Step 3 — Send CREATE_TOPIC manually (PowerShell or Python):**

```python
import socket
s = socket.create_connection(("127.0.0.1", 5001))
s.sendall(b"CREATE_TOPIC demo.server1\n")
print(s.recv(100))   # b'ACK\n'
s.close()
```

**Step 4 — Send a METRIC:**

```python
s = socket.create_connection(("127.0.0.1", 5001))
s.sendall(b"METRIC demo.server1 cpu=55.0 memory=32.1\n")
print(s.recv(100))   # b'ACK\n'
s.close()
```

**Step 5 — Read the metric back:**

```python
s = socket.create_connection(("127.0.0.1", 5001))
s.sendall(b"SUBSCRIBE demo.server1 0\n")
header = s.recv(100)   # b'DATA 62\n'  (new_offset = 62)
data   = s.recv(1000)  # b'{"ts": "...", "data": "cpu=55.0 memory=32.1"}\n'
s.close()
```

**Step 6 — Verify all 3 nodes have the same file:**

```bash
cat broker_data/node0/demo_server1.log
cat broker_data/node1/demo_server1.log
cat broker_data/node2/demo_server1.log
# All three should be identical
```

**Step 7 — Try sending to a follower (should get NACK):**

```python
# Node 0 is a follower
s = socket.create_connection(("127.0.0.1", 5000))
s.sendall(b"METRIC demo.server1 cpu=10.0 memory=30.0\n")
print(s.recv(100))   # b'NACK 5001\n'  ← "go to port 5001 (the leader)"
s.close()
```

---

## Integration Points

**What this part provides to Part 3 (Publisher/Subscriber):**

- A TCP server on each broker's port
- ACK on successful commit
- NACK with leader port for redirection
- DATA response for SUBSCRIBE requests

**What this part provides to Part 1 (RAFT):**

- `LogStore` object so RAFT can write committed entries to disk
- `_send_to_peer()` function so RAFT can contact peer brokers

**What this part calls on Part 1 (RAFT):**

- `raft.is_leader()` — before accepting writes
- `raft.get_leader_port()` — to build NACK response
- `raft.propose(type, topic, data)` — to submit a write
- `raft.wait_for_commit(index)` — to wait for durability confirmation
- All `raft.handle_*()` methods — to dispatch RAFT TCP messages

