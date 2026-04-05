# =============================================================================
# subscriber/log_manager.py
#
# Manages the subscriber's local copy of topic logs.
#
# WHY a local copy?
#   DSMS_SUB acts as a "read-through gateway".  Many clients (dashboards,
#   alert tools) can all query DSMS_SUB instead of hammering the broker
#   directly.  DSMS_SUB polls the broker periodically and caches the result
#   locally.  This also allows DSMS_SUB to do AGGREGATION:
#
# AGGREGATION EXAMPLE:
#   Broker stores:  app1.server1 log, app1.server2 log  (leaf topics only)
#   DSMS_SUB can:   serve a combined "app1" view = server1 entries + server2 entries
#
#   So if you subscribe to "app1", you get data from ALL topics that start
#   with "app1." merged together in one response.
#
# DATA MODEL:
#   self.subscriptions = {
#       "app1.server1": {
#           "byte_offset": 0,           # where we last read up to on the broker
#           "entries":     [...]        # list of parsed log entry dicts
#       },
#       ...
#   }
# =============================================================================

import json
import threading


class LogManager:
    """
    In-memory store for all topic logs fetched from the broker.
    Thread-safe: all mutations go through self._lock.
    """

    def __init__(self):
        # Dictionary mapping topic name → subscription state + cached entries
        self.subscriptions: dict = {}
        self._lock = threading.Lock()

    # =========================================================================
    # Subscription Management
    # =========================================================================

    def subscribe(self, topic: str):
        """
        Register interest in a topic.
        If already subscribed, this is a no-op (idempotent).
        """
        with self._lock:
            if topic not in self.subscriptions:
                self.subscriptions[topic] = {
                    "byte_offset": 0,   # start reading from beginning of log
                    "entries":     [],  # cached log entries (list of dicts)
                }
                print(f"[LogManager] Subscribed to '{topic}'")

    def unsubscribe(self, topic: str):
        """Remove a topic subscription and discard its cached data."""
        with self._lock:
            if topic in self.subscriptions:
                del self.subscriptions[topic]
                print(f"[LogManager] Unsubscribed from '{topic}'")

    def is_subscribed(self, topic: str) -> bool:
        with self._lock:
            return topic in self.subscriptions

    def all_subscribed_topics(self) -> list:
        """Return list of all topics currently subscribed to."""
        with self._lock:
            return list(self.subscriptions.keys())

    # =========================================================================
    # Reading / Writing Cached Data
    # =========================================================================

    def get_byte_offset(self, topic: str) -> int:
        """
        Return the current byte offset for a topic.
        This is sent to the broker in the SUBSCRIBE request so only NEW
        entries are returned (avoids re-reading the whole log every poll).
        """
        with self._lock:
            return self.subscriptions.get(topic, {}).get("byte_offset", 0)

    def update_offset(self, topic: str, new_offset: int):
        """After receiving DATA from the broker, update our byte offset."""
        with self._lock:
            if topic in self.subscriptions:
                self.subscriptions[topic]["byte_offset"] = new_offset

    def add_raw_bytes(self, topic: str, raw_bytes: bytes):
        """
        Parse raw log bytes (one JSON object per line) received from the broker
        and append them to the in-memory entry list for this topic.

        Each line from the broker looks like:
            {"ts": "2026-04-05 10:30:01", "data": "cpu=70.5 memory=45.2"}
        We add a "topic" field so we can distinguish entries when aggregating.
        """
        if not raw_bytes:
            return

        lines = raw_bytes.decode("utf-8", errors="replace").splitlines()
        new_entries = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                entry["topic"] = topic  # tag with source topic for aggregation
                new_entries.append(entry)
            except json.JSONDecodeError:
                pass  # skip malformed lines

        with self._lock:
            if topic in self.subscriptions:
                self.subscriptions[topic]["entries"].extend(new_entries)

        if new_entries:
            print(f"[LogManager] Added {len(new_entries)} new entries for '{topic}'")

    # =========================================================================
    # Query Interface (called by the HTTP API)
    # =========================================================================

    def get_entries(self, topic_or_prefix: str) -> list:
        """
        Return all cached log entries for a topic or topic prefix/suffix.

        Topics follow the format:  <app>.<server>
        e.g.  webapp.server1,  database.server1,  webapp.server2

        Three matching modes are supported:

        1. Exact match     → get_entries("webapp.server1")
                              returns only that specific leaf topic.

        2. PREFIX (app)    → get_entries("webapp")
                              returns entries from ALL topics starting with "webapp."
                              e.g. webapp.server1, webapp.server2
                              Use this to aggregate by application.

        3. SUFFIX (server) → get_entries("server1")
                              returns entries from ALL topics ending with ".server1"
                              e.g. webapp.server1, database.server1
                              Use this to aggregate by server / host.

        This matches the spec:
            "Subscribers can also opt to receive the full aggregated data
             like app1 or server1."

        Returns a list of entry dicts sorted by 'ts' (timestamp) ascending.
        """
        with self._lock:
            result = []
            for t, state in self.subscriptions.items():
                match = (
                    t == topic_or_prefix                    # exact match
                    or t.startswith(topic_or_prefix + ".")  # prefix: "app1" → "app1.server1"
                    or t.endswith("." + topic_or_prefix)    # suffix: "server1" → "app1.server1"
                )
                if match:
                    result.extend(state["entries"])

        # Sort by timestamp string (works because format is YYYY-MM-DD HH:MM:SS)
        result.sort(key=lambda e: e.get("ts", ""))
        return result

    def get_all_status(self) -> dict:
        """
        Return a summary of all subscriptions: topic, entry count, current offset.
        Used by the /status HTTP endpoint for debugging.
        """
        with self._lock:
            return {
                topic: {
                    "entry_count": len(state["entries"]),
                    "byte_offset": state["byte_offset"],
                }
                for topic, state in self.subscriptions.items()
            }
