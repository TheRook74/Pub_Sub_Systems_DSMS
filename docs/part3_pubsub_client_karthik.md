# Part 3 — Publisher & Subscriber (Client Layer)
### Owner: Karthik
### Files: `publisher/dsms_pub.py` · `publisher/metrics.py` · `publisher/apps.json` · `subscriber/dsms_sub.py` · `subscriber/log_manager.py`

---

## What This Part Is Responsible For

This part is the **application layer** — the two client-facing components of the pub-sub system:

1. **Publisher (`DSMS_PUB`)**: Runs on a server machine, collects CPU and memory metrics using `psutil`, and sends them to the broker cluster.
2. **Subscriber (`DSMS_SUB`)**: Pulls metric data from the broker cluster and exposes it via an HTTP REST API so dashboards, alert tools, or humans can query it.

This is the part that makes the system useful. Without it, the broker cluster just sits there doing nothing. This part is also where the **pub-sub model** is most visible: publishers and subscribers are completely decoupled — neither knows the other exists.

---

## How It Fits Into the Whole System

```
[This machine running apps]                    [Any machine / browser]
        │                                               │
   Publisher                                      Subscriber
   dsms_pub.py                                    dsms_sub.py
        │  METRIC/CREATE_TOPIC (TCP)                    │  /logs?topic=... (HTTP)
        ▼                                               │
   Broker Cluster (Part 2)                             │
        │  committed to disk                           │
        │  READ via SUBSCRIBE (TCP) ◄──────────────────┘
        ▼
   broker_data/ log files
```

Publisher → Broker (TCP write) → Broker → Subscriber (TCP pull) → HTTP client (HTTP GET)

---

## File 1: `publisher/apps.json`

### Purpose
Configures what this publisher monitors. You change this file to monitor different applications.

```json
{
  "server_id": "server1",
  "apps": ["python", "chrome", "code"]
}
```

- `server_id`: identifies this machine in topic names. Topics become `python.server1`, `chrome.server1`, `code.server1`.
- `apps`: list of process names to monitor with `psutil`. If a process isn't found, system-wide metrics are used instead.

---

## File 2: `publisher/metrics.py`

### Purpose
Uses the `psutil` library to collect CPU and memory usage from the operating system.

### `get_metrics_for_app(app_name)`
Scans all running processes to find one whose name contains `app_name` (case-insensitive):
```python
for proc in psutil.process_iter(["name", "cpu_percent", "memory_info"]):
    if app_name.lower() in proc.info["name"].lower():
        cpu = proc.cpu_percent(interval=0.1)
        mem_mb = proc.memory_info().rss / (1024 * 1024)
        return f"cpu={round(cpu,1)} memory={round(mem_mb,1)}"
```
Returns a string like `"cpu=5.2 memory=48.6"`, or `None` if the app isn't running.

### `get_system_metrics()`
Falls back to whole-system metrics:
```python
cpu = psutil.cpu_percent(interval=0.1)
mem = psutil.virtual_memory().percent
return f"cpu={cpu} memory={mem}"
```

---

## File 3: `publisher/dsms_pub.py`

### Publisher Startup Sequence

```
1. Read apps.json → get server_id and app list
2. Build topic names: ["python.server1", "chrome.server1", "code.server1"]
3. For each topic: send CREATE_TOPIC to broker cluster
   (retry up to 10 times — cluster might be electing a leader at startup)
4. Enter publish loop:
   every 5 seconds:
     for each app:
       collect metrics  →  send METRIC to broker
```

### Leader Discovery — How the Publisher Finds the Leader

The publisher doesn't know which of the 3 brokers is the leader. It uses this strategy:

```
Try each broker in order:
  If broker responds ACK  → it's the leader! Cache its port.
  If broker responds NACK <port>  → "I'm not the leader, try port X"
                              → immediately retry at the given leader port
  If broker is unreachable  → mark as dead, try next broker
```

The cached leader port is per-topic (`self.topic_leader` dict). If the leader changes (failover), the publisher gets NACK or a connection error on the next send, clears the cache, and scans again.

### Key Design: `_send_request(message, topic)`

This is the core function. It handles:
1. **Known leader**: tries the cached leader port first to avoid NACK round-trips
2. **NACK redirect**: on NACK, reconnects directly to the leader port given
3. **Dead ports**: tracks failed ports within a single call; skips them on NACK redirect
4. **Timeout**: 8 seconds — deliberately longer than broker's commit timeout (6s) so we always wait long enough to get the ACK even after a failover

```python
CONN_TIMEOUT = 8.0   # > broker's wait_for_commit timeout (6.0s)
dead_ports = set()   # ports that failed in this call

for host, port in candidates:
    try:
        with socket.create_connection((host, port), timeout=CONN_TIMEOUT) as s:
            s.sendall(message.encode())
            response = protocol.recv_line(s)
        if response == "ACK":
            return "ACK"
        elif response starts with "NACK":
            leader_port = ...
            retry directly at leader_port
    except ConnectionError:
        dead_ports.add(port)
```

---

## File 4: `subscriber/log_manager.py`

### Purpose
In-memory cache for all metric entries received from brokers. The background poller writes to this cache; the HTTP API reads from it. This means HTTP queries are instantaneous — they never go to the broker.

### Key State
```python
self._entries = {}       # topic → list of dicts [{ts, data}, ...]
self._offsets = {}       # topic → current byte offset (where we read up to)
self._subscribed = set() # topics currently being polled
```

### `subscribe(topic)` / `unsubscribe(topic)`
Adds/removes from `_subscribed`. The poller thread checks `is_subscribed()` in its loop condition and stops itself when removed.

### `add_raw_bytes(topic, raw_bytes)`
Called by the poller after receiving DATA from the broker. Parses each JSON line:
```python
for line in raw_bytes.decode().splitlines():
    entry = json.loads(line)   # {"ts": "...", "data": "..."}
    self._entries[topic].append(entry)
```

### `get_entries(topic_or_prefix)` — Topic Aggregation
This is the most interesting function. It supports three query modes:

```python
for t in self._entries:
    match = (
        t == topic_or_prefix              # exact: "python.server1"
        or t.startswith(topic_or_prefix + ".")  # app prefix: "python" → all python.*
        or t.endswith("." + topic_or_prefix)    # server suffix: "server1" → all *.server1
    )
```

| Query | What you get |
|---|---|
| `?topic=python.server1` | Only that exact topic |
| `?topic=python` | All apps named python on any server (python.server1, python.server2, ...) |
| `?topic=server1` | All apps monitored on server1 (python.server1, chrome.server1, code.server1) |

---

## File 5: `subscriber/dsms_sub.py`

### Two Concurrent Parts

The subscriber runs two things at the same time:

```
Main thread  → Flask HTTP server (blocks serving HTTP requests)
Background threads → one poller per subscribed topic (fetch data from broker)
```

### HTTP API Endpoints

| Endpoint | Method | What it does |
|---|---|---|
| `/subscribe?topic=X` | POST | Start polling topic X from broker |
| `/subscribe?topic=X` | DELETE | Stop polling topic X |
| `/logs?topic=X` | GET | Return all cached entries for X (or aggregated) |
| `/status` | GET | Show all subscriptions and entry counts |

### Background Poller — `_poll_loop(topic)`

```python
while self.log_mgr.is_subscribed(topic):
    self._fetch_new_entries(topic)
    time.sleep(config.POLL_INTERVAL)   # 3 seconds
```

One thread per subscribed topic. Stops automatically when the topic is unsubscribed. If the broker is unreachable, it prints a warning and retries on the next cycle.

### `_fetch_new_entries(topic)` — The Pull Protocol

```
1. Get current byte offset from LogManager
2. Send: SUBSCRIBE <topic> <offset>\n
3. Receive: DATA <new_offset>\n
4. Receive: raw bytes until connection closes
5. Update offset in LogManager
6. Parse and store entries in LogManager
```

If the broker responds NACK (not leader), the subscriber records the redirected leader port and continues. Like the publisher, it caches the known leader port to avoid NACK round-trips on future polls.

### Why Pull Instead of Push?

The subscriber actively fetches data at its own pace (pull model). This means:
- The subscriber controls how fast it reads — no risk of being overwhelmed
- The broker never needs to maintain a list of subscribers (stateless)
- A subscriber can catch up on historical data by starting from offset=0
- Multiple subscribers can read the same data independently at different speeds

---

## How to Demo This Part Independently

### Publisher Demo

**Prerequisite**: brokers must be running (Part 2).

**Step 1 — Edit `publisher/apps.json`** to monitor processes on your machine:
```json
{
  "server_id": "myserver",
  "apps": ["python", "chrome"]
}
```

**Step 2 — Start the publisher:**
```bash
cd dsms
python publisher/dsms_pub.py
```

**Expected output:**
```
[PUB] Server ID   : myserver
[PUB] Apps        : ['python', 'chrome']
[PUB] Topics      : ['python.myserver', 'chrome.myserver']
[PUB] Creating topic 'python.myserver' (attempt 1/10)
[PUB] NACK from port 5000 → leader is port 5001
[PUB] Topic 'python.myserver' created successfully.
[PUB] Sending METRIC | topic=python.myserver | cpu=0.0 memory=0.12
[PUB] Sending METRIC | topic=chrome.myserver | cpu=5.2 memory=48.6
```

### Subscriber Demo

**Step 1 — Start the subscriber:**
```bash
python subscriber/dsms_sub.py
```

**Step 2 — Subscribe to a topic (new terminal):**
```bash
# PowerShell:
Invoke-WebRequest -Uri "http://localhost:8080/subscribe?topic=python.myserver" -Method POST

# Linux/Mac:
curl -X POST "http://localhost:8080/subscribe?topic=python.myserver"
```

**Step 3 — Read the data:**
```bash
# Exact topic:
curl "http://localhost:8080/logs?topic=python.myserver"

# All topics on myserver (suffix aggregation):
curl "http://localhost:8080/logs?topic=myserver"

# All python.* topics across all servers (prefix aggregation):
curl "http://localhost:8080/logs?topic=python"
```

**Step 4 — Check subscription status:**
```bash
curl "http://localhost:8080/status"
```

**Expected response:**
```json
{
  "topic": "myserver",
  "entry_count": 42,
  "entries": [
    {"ts": "2026-04-05 22:14:00", "data": "cpu=0.0 memory=0.12"},
    {"ts": "2026-04-05 22:14:05", "data": "cpu=5.2 memory=48.6"},
    ...
  ]
}
```

### Demo: Fault Tolerance from the Client's Perspective

**While publisher is running, kill the broker leader (Ctrl+C on leader terminal).**

Publisher output during failover:
```
[PUB] Cannot reach broker at 127.0.0.1:5001 — [Errno 61] Connection refused
[PUB] NACK from port 5000 → leader is port 5002
[PUB] Sending METRIC | topic=python.myserver | cpu=0.0 memory=0.12
```
Publisher automatically finds the new leader and continues. No data is lost.

Subscriber output during failover:
```
[SUB] Cannot reach broker 127.0.0.1:5001 — timed out
[SUB] ◄ Received 3 new entry/entries for 'python.myserver' (offset 1240 → 1426)
```
Subscriber switches to the new leader and catches up on any entries it missed.

---

## Integration Points

**What this part needs from Part 2 (Broker):**
- A running TCP server on the ports in `config.BROKERS`
- `ACK` response after successful CREATE_TOPIC or METRIC
- `NACK <port>` response when contacting a follower
- `DATA <offset>\n<bytes>` response for SUBSCRIBE requests

**What this part needs from Part 1 (RAFT):**
- Nothing directly — this part only talks to the broker TCP interface
- RAFT is invisible to Publisher and Subscriber

**What this part provides to the end user:**
- Live metric data via `GET /logs?topic=...`
- Historical data replay (start from offset=0)
- Topic aggregation (prefix and suffix matching)
- System health via `GET /status`
