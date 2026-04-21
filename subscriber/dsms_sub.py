# =============================================================================
# subscriber/dsms_sub.py  —  DSMS Subscriber Application (DSMS_SUB)
#
# DSMS_SUB is the subscriber side of the pub-sub system.
# It has TWO parts running concurrently:
#
#   PART A — Broker Poller (one background thread per *concrete* topic):
#     Periodically sends SUBSCRIBE <topic> <offset> to the broker leader.
#     The broker returns DATA with new log entries, which are stored in
#     the LogManager.
#
#   PART B — Pattern Reconciler (one background thread total):
#     Every POLL_INTERVAL seconds it asks the broker LIST_TOPICS <pattern>
#     for every active pattern the user subscribed to, and spawns pollers
#     for any newly-created concrete topics.  This is how the subscriber
#     picks up new apps / new servers added while it is already running.
#
#   PART C — HTTP REST API (Flask web server):
#     Clients (dashboards, alert tools, humans) query this HTTP server
#     to read the collected metrics.  No direct broker contact needed.
#
# HTTP API Endpoints:
#   POST   /subscribe?topic=<pattern>  → watch a topic OR a pattern
#   DELETE /subscribe?topic=<pattern>  → stop watching a pattern
#   GET    /logs?topic=<topic>         → get all entries (supports aggregation)
#   GET    /status                     → show all subscriptions + entry counts
#
# Pattern vs. concrete topic:
#   brave.server0   (concrete)    -> one file on disk
#   brave           (app prefix)  -> every brave.* topic
#   server0         (server suffix) -> every *.server0 topic
#
# The subscriber accepts any of the three forms on /subscribe.  It asks
# the broker to expand the pattern via LIST_TOPICS and then manages one
# poller per concrete topic under the hood.  Reference counting keeps
# things tidy when multiple patterns overlap on the same concrete topic.
#
# Example curl commands:
#   curl -X POST   "http://localhost:8080/subscribe?topic=brave.server0"
#   curl -X POST   "http://localhost:8080/subscribe?topic=server0"
#   curl          "http://localhost:8080/logs?topic=server0"
#   curl          "http://localhost:8080/status"
# =============================================================================

import socket
import threading
import time
import sys
import os
import argparse

# Flask is a lightweight Python web framework for building HTTP APIs.
from flask import Flask, request, jsonify

# Allow imports from the parent 'dsms' package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol
from subscriber.log_manager import LogManager


# =============================================================================
# Subscriber class
# =============================================================================

class Subscriber:
    """
    DSMS_SUB: pulls metric data from the broker cluster and exposes it
    via an HTTP REST API.
    """

    def __init__(self, http_port: int = config.SUBSCRIBER_HTTP_PORT):
        self.http_port = http_port
        self.log_mgr = LogManager()

        # Poller threads for concrete topics. topic -> Thread.
        self._poller_threads: dict = {}

        # Patterns the user explicitly subscribed to.
        self._active_patterns: set = set()

        # Reference count: concrete_topic -> set of patterns that claim it.
        # Used when unsubscribing a pattern so we do not kill pollers that
        # another active pattern is still interested in.
        self._topic_refs: dict = {}

        # Guards _active_patterns and _topic_refs (separate from LogManager's lock).
        self._patterns_lock = threading.Lock()

        # Known leader port — starts unknown, filled in after first contact.
        self._leader_port = None
        self._leader_lock = threading.Lock()

        # Set up the Flask HTTP application.
        self.app = Flask(__name__)
        self._register_routes()

        # Kick off the reconciler thread.  It discovers new concrete topics
        # for every active pattern once per POLL_INTERVAL.
        threading.Thread(
            target=self._reconciler_loop,
            daemon=True,
            name="pattern-reconciler",
        ).start()

    # =========================================================================
    # HTTP API Routes
    # =========================================================================

    def _register_routes(self):
        """Attach all URL routes to the Flask app."""
        app = self.app

        # ---- POST /subscribe?topic=<pattern> ----
        @app.route("/subscribe", methods=["POST"])
        def subscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            matched = self._subscribe_pattern(topic)
            return jsonify({
                "status": "subscribed",
                "pattern": topic,
                "matched_topics": matched,
            }), 200

        # ---- DELETE /subscribe?topic=<pattern> ----
        @app.route("/subscribe", methods=["DELETE"])
        def unsubscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            removed = self._unsubscribe_pattern(topic)
            return jsonify({
                "status": "unsubscribed",
                "pattern": topic,
                "dropped_topics": removed,
            }), 200

        # ---- GET /logs?topic=<topic> ----
        @app.route("/logs", methods=["GET"])
        def get_logs():
            """
            Return all cached log entries for a topic or topic prefix/suffix.

            ?topic=python.server1   → only that exact leaf topic
            ?topic=python           → aggregated from ALL python.* topics
            ?topic=server1          → aggregated from ALL *.server1 topics
            """
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            entries = self.log_mgr.get_entries(topic)
            return jsonify({
                "topic":       topic,
                "entry_count": len(entries),
                "entries":     entries,
            }), 200

        # ---- GET /status ----
        @app.route("/status", methods=["GET"])
        def get_status():
            """Show all active subscriptions and how many entries are cached."""
            with self._patterns_lock:
                patterns = sorted(self._active_patterns)
            return jsonify({
                "patterns":      patterns,
                "subscriptions": self.log_mgr.get_all_status(),
                "leader_port":   self._leader_port,
            }), 200

    # =========================================================================
    # Pattern-level subscribe / unsubscribe
    # =========================================================================

    def _subscribe_pattern(self, pattern: str) -> list:
        """
        Register a pattern, ask the broker which concrete topics match it,
        and start a poller for each one that is not already polled.
        Returns the list of concrete topics that were attached.
        """
        with self._patterns_lock:
            self._active_patterns.add(pattern)

        # Network call is done outside the lock so HTTP calls never block each
        # other on a slow broker.
        concrete = self._list_topics_from_broker(pattern)

        self._attach_concrete_topics(pattern, concrete)

        if not concrete:
            # Pattern is valid but nothing matches yet (e.g. a server whose
            # publishers have not started).  Reconciler will pick it up later.
            print(f"[SUB] Pattern '{pattern}' registered; no matching topics yet")
        else:
            print(f"[SUB] Pattern '{pattern}' expanded to {len(concrete)} topic(s): {concrete}")

        return concrete

    def _unsubscribe_pattern(self, pattern: str) -> list:
        """
        Drop a pattern.  For each concrete topic whose last remaining
        claiming pattern was this one, stop its poller and discard cached
        entries.  Returns the list of topics that were dropped.
        """
        dropped = []
        with self._patterns_lock:
            self._active_patterns.discard(pattern)

            # Walk the ref table and remove this pattern from each entry.
            topics_to_check = list(self._topic_refs.keys())
            for topic in topics_to_check:
                refs = self._topic_refs.get(topic, set())
                refs.discard(pattern)
                if not refs:
                    # Nobody still wants this concrete topic — unsubscribe it.
                    self._topic_refs.pop(topic, None)
                    dropped.append(topic)

        # LogManager.unsubscribe is idempotent and its poll loop exits on
        # the next iteration when is_subscribed returns False.
        for topic in dropped:
            self.log_mgr.unsubscribe(topic)

        if dropped:
            print(f"[SUB] Pattern '{pattern}' dropped, stopped {len(dropped)} poller(s): {dropped}")
        else:
            print(f"[SUB] Pattern '{pattern}' dropped (no pollers to stop)")

        return dropped

    def _attach_concrete_topics(self, pattern: str, concrete: list):
        """
        Shared helper: add a pattern ref for each concrete topic and make
        sure a poller exists.  Called from both the initial subscribe and
        the periodic reconciler.
        """
        new_pollers = []
        with self._patterns_lock:
            for topic in concrete:
                refs = self._topic_refs.setdefault(topic, set())
                if pattern not in refs:
                    refs.add(pattern)
                # LogManager.subscribe is idempotent.
                # _start_poller is idempotent (checks _poller_threads).
                if topic not in self._poller_threads:
                    new_pollers.append(topic)

        for topic in new_pollers:
            self.log_mgr.subscribe(topic)
            self._start_poller(topic)

    # =========================================================================
    # Reconciler — picks up topics created AFTER a pattern was registered
    # =========================================================================

    def _reconciler_loop(self):
        """
        Runs forever in the background.  Every POLL_INTERVAL seconds it asks
        the broker LIST_TOPICS for each active pattern and attaches any new
        concrete topics.  This is the only place where a late-joining publisher
        is discovered.
        """
        while True:
            time.sleep(config.POLL_INTERVAL)
            try:
                with self._patterns_lock:
                    patterns = list(self._active_patterns)
                for pattern in patterns:
                    concrete = self._list_topics_from_broker(pattern)
                    if concrete:
                        self._attach_concrete_topics(pattern, concrete)
            except Exception as e:
                print(f"[SUB] Reconciler error (continuing): {e}")

    # =========================================================================
    # Broker call: LIST_TOPICS
    # =========================================================================

    def _list_topics_from_broker(self, pattern: str) -> list:
        """
        Ask the broker leader for every concrete topic that matches a pattern.
        Handles NACK redirects and falls back through config.BROKERS.
        Returns an empty list if every broker is unreachable or the pattern
        matches nothing.
        """
        message = protocol.encode_list_topics(pattern)

        for host, port in self._get_broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as s:
                    s.sendall(message.encode())
                    line = protocol.recv_line(s)

                if line is None:
                    continue
                msg_type, payload = protocol.decode_message(line)

                if msg_type == "NACK":
                    leader_port = payload.get("leader_port", -1)
                    if leader_port > 0:
                        with self._leader_lock:
                            self._leader_port = leader_port
                    continue  # retry with updated leader

                if msg_type == "TOPICS":
                    with self._leader_lock:
                        self._leader_port = port
                    return payload.get("topics", [])

                print(f"[SUB] Unexpected LIST_TOPICS response: {msg_type}")
                return []

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] LIST_TOPICS cannot reach {host}:{port} — {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None

        return []

    # =========================================================================
    # Single-topic Poller (unchanged behavior from before)
    # =========================================================================

    def _start_poller(self, topic: str):
        """
        Start a background thread that periodically polls the broker
        for new log entries for 'topic'.  Idempotent.
        """
        if topic in self._poller_threads:
            t = self._poller_threads[topic]
            if t.is_alive():
                return  # already running

        t = threading.Thread(
            target=self._poll_loop,
            args=(topic,),
            daemon=True,
            name=f"poller-{topic}",
        )
        self._poller_threads[topic] = t
        t.start()
        print(f"[SUB] Started poller thread for '{topic}'")

    def _poll_loop(self, topic: str):
        """
        Every POLL_INTERVAL seconds, fetch new entries for this concrete topic.
        Stops automatically when the topic is unsubscribed.
        """
        print(f"[SUB] Poller active for '{topic}'")
        while self.log_mgr.is_subscribed(topic):
            try:
                self._fetch_new_entries(topic)
            except Exception as e:
                print(f"[SUB] Poller error for '{topic}': {e}")
            time.sleep(config.POLL_INTERVAL)
        # Clean up so _start_poller can restart this topic later if needed.
        self._poller_threads.pop(topic, None)
        print(f"[SUB] Poller stopped for '{topic}'")

    def _fetch_new_entries(self, topic: str):
        """
        Send one SUBSCRIBE request to the broker and process the DATA response.

        Protocol:
          → SUBSCRIBE <topic> <byte_offset>\n
          ← DATA <new_offset>\n
            <raw log bytes (zero or more JSON lines)>
          ← [connection closed by broker]
        """
        byte_offset = self.log_mgr.get_byte_offset(topic)
        message = protocol.encode_subscribe(topic, byte_offset)

        for host, port in self._get_broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as s:
                    s.sendall(message.encode())

                    header_line = protocol.recv_line(s)
                    if header_line is None:
                        continue

                    msg_type, payload = protocol.decode_message(header_line)

                    if msg_type == "NACK":
                        leader_port = payload.get("leader_port", -1)
                        if leader_port > 0:
                            with self._leader_lock:
                                self._leader_port = leader_port
                        continue

                    if msg_type != "DATA":
                        print(f"[SUB] Unexpected response type: {msg_type}")
                        continue

                    new_offset = payload["new_offset"]

                    raw_content = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        raw_content += chunk

                    with self._leader_lock:
                        self._leader_port = port

                    self.log_mgr.update_offset(topic, new_offset)
                    self.log_mgr.add_raw_bytes(topic, raw_content)

                    if raw_content:
                        lines = [l for l in raw_content.decode(
                            "utf-8", errors="replace").splitlines() if l.strip()]
                        print(f"[SUB] ◄ Received {len(lines)} new entry/entries "
                              f"for '{topic}' (offset {byte_offset} → {new_offset})")
                        for line in lines:
                            print(f"       {line}")
                    else:
                        print(f"[SUB] ◄ Poll '{topic}' — no new data "
                              f"(offset={byte_offset})")

                    return

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] Cannot reach broker {host}:{port} — {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None

    # =========================================================================
    # Helpers
    # =========================================================================

    def _get_broker_candidates(self) -> list:
        """Known leader first, then the rest of the brokers as fallback."""
        with self._leader_lock:
            lp = self._leader_port

        candidates = []
        if lp:
            lh = self._port_to_host(lp)
            candidates.append((lh, lp))
        for b in config.BROKERS:
            if b["port"] != lp:
                candidates.append((b["host"], b["port"]))
        return candidates

    def _port_to_host(self, port: int) -> str:
        for b in config.BROKERS:
            if b["port"] == port:
                return b["host"]
        return config.BROKERS[0]["host"]

    # =========================================================================
    # Run
    # =========================================================================

    def run(self):
        """Start the Flask HTTP server (blocking)."""
        print(f"[SUB] HTTP API running at http://127.0.0.1:{self.http_port}")
        print(f"[SUB] Subscribe to a topic (exact, app-prefix, or server-suffix):")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave.server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave'")
        self.app.run(host="0.0.0.0", port=self.http_port,
                     debug=False, use_reloader=False)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSMS Subscriber — HTTP API for reading metrics.")
    parser.add_argument(
        "--port", type=int, default=config.SUBSCRIBER_HTTP_PORT,
        help=f"HTTP port for the REST API (default: {config.SUBSCRIBER_HTTP_PORT})",
    )
    args = parser.parse_args()

    sub = Subscriber(http_port=args.port)
    sub.run()
