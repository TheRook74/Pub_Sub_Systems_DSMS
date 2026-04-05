# =============================================================================
# subscriber/dsms_sub.py  —  DSMS Subscriber Application (DSMS_SUB)
#
# DSMS_SUB is the subscriber side of the pub-sub system.
# It has TWO parts running concurrently:
#
#   PART A — Broker Poller (background thread per subscribed topic):
#     Periodically sends SUBSCRIBE <topic> <offset> to the broker leader.
#     The broker returns DATA with new log entries.
#     These are stored locally in the LogManager.
#
#   PART B — HTTP REST API (Flask web server):
#     Clients (dashboards, alert tools, humans) query this HTTP server
#     to read the collected metrics.  No direct broker contact needed.
#
# HTTP API Endpoints:
#   POST   /subscribe?topic=<topic>    → start watching a topic
#   DELETE /subscribe?topic=<topic>    → stop watching a topic
#   GET    /logs?topic=<topic>         → get all entries (supports prefix agg.)
#   GET    /status                     → show all subscriptions + entry counts
#
# Usage:
#   python dsms_sub.py
#   python dsms_sub.py --port 8080
#
# Example curl commands:
#   curl -X POST   "http://localhost:8080/subscribe?topic=python.server1"
#   curl -X POST   "http://localhost:8080/subscribe?topic=chrome.server1"
#   curl          "http://localhost:8080/logs?topic=python.server1"
#   curl          "http://localhost:8080/logs?topic=server1"   # aggregate all
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
        self.http_port  = http_port
        self.log_mgr    = LogManager()

        # Tracks background poller threads (topic → Thread)
        self._poller_threads: dict = {}

        # Known leader port — starts unknown, filled in after first contact
        self._leader_port = None
        self._leader_lock = threading.Lock()

        # Set up the Flask HTTP application
        self.app = Flask(__name__)
        self._register_routes()

    # =========================================================================
    # HTTP API Routes
    # =========================================================================

    def _register_routes(self):
        """Attach all URL routes to the Flask app."""
        app = self.app

        # ---- POST /subscribe?topic=<topic> ----
        @app.route("/subscribe", methods=["POST"])
        def subscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            self.log_mgr.subscribe(topic)
            self._start_poller(topic)
            return jsonify({"status": "subscribed", "topic": topic}), 200

        # ---- DELETE /subscribe?topic=<topic> ----
        @app.route("/subscribe", methods=["DELETE"])
        def unsubscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            self.log_mgr.unsubscribe(topic)
            # The poller thread checks log_mgr.is_subscribed() and stops itself
            return jsonify({"status": "unsubscribed", "topic": topic}), 200

        # ---- GET /logs?topic=<topic> ----
        @app.route("/logs", methods=["GET"])
        def get_logs():
            """
            Return all cached log entries for a topic or topic prefix.

            ?topic=python.server1   → only that exact leaf topic
            ?topic=python           → aggregated from ALL python.* topics
            ?topic=server1          → would match python.server1, chrome.server1, etc.
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
            return jsonify({
                "subscriptions": self.log_mgr.get_all_status(),
                "leader_port":   self._leader_port,
            }), 200

    # =========================================================================
    # Background Polling
    # =========================================================================

    def _start_poller(self, topic: str):
        """
        Start a background thread that periodically polls the broker
        for new log entries for 'topic'.
        Does nothing if a poller for this topic is already running.
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
        Runs in a background thread.
        Every POLL_INTERVAL seconds, sends SUBSCRIBE <topic> <offset> to the
        broker leader and appends any new entries to the LogManager.
        Stops automatically when the topic is unsubscribed.
        """
        print(f"[SUB] Poller active for '{topic}'")
        while self.log_mgr.is_subscribed(topic):
            try:
                self._fetch_new_entries(topic)
            except Exception as e:
                print(f"[SUB] Poller error for '{topic}': {e}")
            time.sleep(config.POLL_INTERVAL)
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
        message     = protocol.encode_subscribe(topic, byte_offset)

        # Try the known leader first, then fall back to all brokers
        candidates = self._get_broker_candidates()

        for host, port in candidates:
            try:
                with socket.create_connection((host, port), timeout=5.0) as s:
                    s.sendall(message.encode())

                    # Read the header line: "DATA <new_offset>\n"
                    header_line = protocol.recv_line(s)
                    if header_line is None:
                        continue

                    msg_type, payload = protocol.decode_message(header_line)

                    if msg_type == "NACK":
                        # This broker told us who the leader is
                        leader_port = payload.get("leader_port", -1)
                        if leader_port > 0:
                            with self._leader_lock:
                                self._leader_port = leader_port
                        continue  # retry with updated leader

                    if msg_type != "DATA":
                        print(f"[SUB] Unexpected response type: {msg_type}")
                        continue

                    new_offset = payload["new_offset"]

                    # Read remaining bytes (the actual log content)
                    # The broker closes the connection after sending, so
                    # we just read until EOF.
                    raw_content = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        raw_content += chunk

                    # Cache this port as the known leader
                    with self._leader_lock:
                        self._leader_port = port

                    # Update offset and store entries
                    self.log_mgr.update_offset(topic, new_offset)
                    self.log_mgr.add_raw_bytes(topic, raw_content)

                    # Print what was received so it's visible in the terminal
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

                    return  # success — no need to try other brokers

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] Cannot reach broker {host}:{port} — {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None  # leader is down

        # All brokers failed this round; will retry on next poll cycle

    # =========================================================================
    # Helpers
    # =========================================================================

    def _get_broker_candidates(self) -> list:
        """
        Return (host, port) pairs to try for a SUBSCRIBE request.
        Known leader goes first to avoid unnecessary NACK round-trips.
        """
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
        print(f"[SUB] Subscribe to a topic with:")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=python.server1'")
        # use_reloader=False is important when running alongside background threads
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
