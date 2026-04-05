# =============================================================================
# common/protocol.py
#
# Message encoding and decoding for all TCP communication in DSMS.
#
# ALL messages are plain-text lines terminated by a newline character ("\n").
# This keeps the protocol simple and human-readable.
#
# Message format overview:
#   CLIENT <-> BROKER:
#       CREATE_TOPIC <topic>
#       METRIC <topic> <data>
#       ACK
#       NACK <leader_port>          (follower tells client who the leader is)
#       SUBSCRIBE <topic> <offset>
#       DATA <new_offset>           (followed immediately by raw log bytes)
#
#   RAFT INTERNAL (broker <-> broker):
#       VOTE_REQUEST  {json payload}
#       VOTE_GRANTED  {json payload}
#       VOTE_DENIED   {json payload}
#       REPLICATE     {json payload}   (also acts as heartbeat when entries=[])
#       REPLICATE_ACK {json payload}
# =============================================================================

import json


# =============================================================================
# ENCODING  –  create a message string ready to send over the socket
# =============================================================================

def encode_create_topic(topic: str) -> str:
    """Publisher requests broker to create a new topic log."""
    return f"CREATE_TOPIC {topic}\n"


def encode_metric(topic: str, data: str) -> str:
    """Publisher sends a metric measurement to the broker leader."""
    return f"METRIC {topic} {data}\n"


def encode_ack() -> str:
    """Broker confirms that a request was successfully committed."""
    return "ACK\n"


def encode_nack(leader_port) -> str:
    """
    A follower broker tells the client it is NOT the leader.
    leader_port: TCP port of the current leader, or -1 if no leader is known.
    """
    return f"NACK {leader_port}\n"


def encode_subscribe(topic: str, byte_offset: int) -> str:
    """
    Subscriber asks for new log data starting at byte_offset.
    byte_offset=0 means 'give me everything from the beginning'.
    On subsequent polls, the subscriber sends back the new_offset it got
    from the previous DATA response, so it only fetches NEW entries.
    """
    return f"SUBSCRIBE {topic} {byte_offset}\n"


def encode_data_header(new_offset: int) -> bytes:
    """
    Broker sends this line before the raw log content.
    new_offset tells the subscriber where to start reading next time.
    """
    return f"DATA {new_offset}\n".encode()


# ---- RAFT Internal Messages ----

def encode_vote_request(term: int, candidate_id: int,
                        last_log_index: int, last_log_term: int) -> str:
    """
    Candidate asks peer nodes to vote for it.
    last_log_index / last_log_term: candidate's last log entry info.
    Peers use this to decide if the candidate is up-to-date enough.
    """
    payload = json.dumps({
        "term": term,
        "candidate_id": candidate_id,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    })
    return f"VOTE_REQUEST {payload}\n"


def encode_vote_granted(term: int, voter_id: int) -> str:
    """Node grants its vote to the candidate."""
    return f"VOTE_GRANTED {json.dumps({'term': term, 'voter_id': voter_id})}\n"


def encode_vote_denied(term: int) -> str:
    """Node refuses to vote for the candidate."""
    return f"VOTE_DENIED {json.dumps({'term': term})}\n"


def encode_replicate(term: int, leader_id: int, prev_log_index: int,
                     prev_log_term: int, entries: list,
                     commit_index: int) -> str:
    """
    Leader sends log entries to a follower.
    When entries=[] this acts as a pure heartbeat (no new data, just 'I'm alive').
    prev_log_index / prev_log_term: the entry just before the new ones,
    used by the follower to verify its log is consistent with the leader's.
    commit_index: how far the leader has committed, so follower can advance too.
    """
    payload = json.dumps({
        "term": term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "commit_index": commit_index,
    })
    return f"REPLICATE {payload}\n"


def encode_replicate_ack(term: int, follower_id: int,
                         match_index: int, success: bool) -> str:
    """
    Follower acknowledges a REPLICATE message.
    success=True:  entries were accepted, match_index = highest stored index.
    success=False: log was inconsistent, leader should back up and retry.
    """
    payload = json.dumps({
        "term": term,
        "follower_id": follower_id,
        "match_index": match_index,
        "success": success,
    })
    return f"REPLICATE_ACK {payload}\n"


# =============================================================================
# DECODING  –  parse a raw message line into (msg_type, payload_dict)
# =============================================================================

def decode_message(line: str):
    """
    Parse a single newline-terminated message string.
    Returns (msg_type: str, payload: dict).
    Returns (None, None) if the line is empty or cannot be parsed.
    """
    if not line:
        return None, None

    line = line.strip()
    if not line:
        return None, None

    # Split on the first space to separate the message type from the payload
    parts = line.split(" ", 1)
    msg_type = parts[0].upper()
    rest = parts[1] if len(parts) > 1 else ""

    # ---------- Plain text messages ----------

    if msg_type == "ACK":
        return "ACK", {}

    if msg_type == "NACK":
        # rest is just the leader port number
        try:
            return "NACK", {"leader_port": int(rest.strip())}
        except ValueError:
            return "NACK", {"leader_port": -1}

    if msg_type == "CREATE_TOPIC":
        return "CREATE_TOPIC", {"topic": rest.strip()}

    if msg_type == "METRIC":
        # Format: METRIC <topic> <data...>
        sub = rest.split(" ", 1)
        topic = sub[0]
        data  = sub[1] if len(sub) > 1 else ""
        return "METRIC", {"topic": topic, "data": data}

    if msg_type == "SUBSCRIBE":
        # Format: SUBSCRIBE <topic> <offset>
        sub = rest.split(" ", 1)
        topic  = sub[0]
        offset = int(sub[1]) if len(sub) > 1 else 0
        return "SUBSCRIBE", {"topic": topic, "offset": offset}

    if msg_type == "DATA":
        # Format: DATA <new_offset>
        try:
            return "DATA", {"new_offset": int(rest.strip())}
        except ValueError:
            return "DATA", {"new_offset": 0}

    # ---------- JSON-payload RAFT messages ----------

    raft_types = {
        "VOTE_REQUEST", "VOTE_GRANTED", "VOTE_DENIED",
        "REPLICATE", "REPLICATE_ACK",
    }
    if msg_type in raft_types:
        try:
            payload = json.loads(rest)
        except json.JSONDecodeError:
            payload = {}
        return msg_type, payload

    # Unknown message type
    return msg_type, {"raw": rest}


# =============================================================================
# Socket Helpers
# =============================================================================

def recv_line(sock) -> str:
    """
    Read bytes from a socket one at a time until a newline is found.
    Returns the full line as a string, or None if the connection was closed.

    Why read one byte at a time?  Because we don't know how long the line is
    in advance, and we don't want to accidentally consume bytes from the NEXT
    message.
    """
    buf = b""
    while True:
        try:
            byte = sock.recv(1)
        except OSError:
            return None
        if not byte:
            # Connection closed by remote side
            return None
        buf += byte
        if buf.endswith(b"\n"):
            return buf.decode("utf-8", errors="replace")
