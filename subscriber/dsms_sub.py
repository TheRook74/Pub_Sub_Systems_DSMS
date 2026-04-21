"""Subscriber service that polls broker topics and exposes cached data via HTTP."""

import argparse
import os
import socket
import sys
import threading
import time

from flask import Flask, jsonify, request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol
from subscriber.log_manager import LogManager


class Subscriber:
    """Manage topic subscriptions, poll broker data, and serve REST endpoints."""

    def __init__(self, http_port: int = config.SUBSCRIBER_HTTP_PORT):
        self.http_port = http_port
        self.log_mgr = LogManager()
        self._poller_threads: dict = {}
        self._active_patterns: set = set()
        self._topic_refs: dict = {}
        self._patterns_lock = threading.Lock()
        self._leader_port = None
        self._leader_lock = threading.Lock()

        self.app = Flask(__name__)
        self._register_routes()

        threading.Thread(
            target=self._reconciler_loop,
            daemon=True,
            name="pattern-reconciler",
        ).start()

    def _register_routes(self):
        """Register HTTP endpoints for subscribe, unsubscribe, logs, and status."""
        app = self.app

        @app.route("/subscribe", methods=["POST"])
        def subscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            matched_topics = self._subscribe_pattern(topic)
            return jsonify({"status": "subscribed", "pattern": topic, "matched_topics": matched_topics}), 200

        @app.route("/subscribe", methods=["DELETE"])
        def unsubscribe():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            dropped_topics = self._unsubscribe_pattern(topic)
            return jsonify({"status": "unsubscribed", "pattern": topic, "dropped_topics": dropped_topics}), 200

        @app.route("/logs", methods=["GET"])
        def get_logs():
            topic = request.args.get("topic", "").strip()
            if not topic:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            entries = self.log_mgr.get_entries(topic)
            return jsonify({"topic": topic, "entry_count": len(entries), "entries": entries}), 200

        @app.route("/status", methods=["GET"])
        def get_status():
            with self._patterns_lock:
                patterns = sorted(self._active_patterns)
            return jsonify({"patterns": patterns, "subscriptions": self.log_mgr.get_all_status(), "leader_port": self._leader_port}), 200

    def _subscribe_pattern(self, pattern: str) -> list:
        """Subscribe to a pattern and attach pollers for all currently matched topics."""
        with self._patterns_lock:
            self._active_patterns.add(pattern)

        concrete_topics = self._list_topics_from_broker(pattern)
        self._attach_concrete_topics(pattern, concrete_topics)

        if not concrete_topics:
            print(f"[SUB] Pattern '{pattern}' registered; no matching topics yet")
        else:
            print(f"[SUB] Pattern '{pattern}' expanded to {len(concrete_topics)} topic(s): {concrete_topics}")

        return concrete_topics

    def _unsubscribe_pattern(self, pattern: str) -> list:
        """Unsubscribe a pattern and stop pollers that are no longer referenced."""
        dropped_topics = []

        with self._patterns_lock:
            self._active_patterns.discard(pattern)
            topics_to_check = list(self._topic_refs.keys())
            for topic in topics_to_check:
                refs = self._topic_refs.get(topic, set())
                refs.discard(pattern)
                if not refs:
                    self._topic_refs.pop(topic, None)
                    dropped_topics.append(topic)

        for topic in dropped_topics:
            self.log_mgr.unsubscribe(topic)

        if dropped_topics:
            print(f"[SUB] Pattern '{pattern}' dropped, stopped {len(dropped_topics)} poller(s): {dropped_topics}")
        else:
            print(f"[SUB] Pattern '{pattern}' dropped (no pollers to stop)")

        return dropped_topics

    def _attach_concrete_topics(self, pattern: str, concrete_topics: list):
        """Attach pattern references and start pollers for newly discovered concrete topics."""
        new_pollers = []

        with self._patterns_lock:
            for topic in concrete_topics:
                refs = self._topic_refs.setdefault(topic, set())
                if pattern not in refs:
                    refs.add(pattern)
                if topic not in self._poller_threads:
                    new_pollers.append(topic)

        for topic in new_pollers:
            self.log_mgr.subscribe(topic)
            self._start_poller(topic)

    def _reconciler_loop(self):
        """Refresh concrete topic expansions for active patterns at poll intervals."""
        while True:
            time.sleep(config.POLL_INTERVAL)
            try:
                with self._patterns_lock:
                    patterns = list(self._active_patterns)
                for pattern in patterns:
                    concrete_topics = self._list_topics_from_broker(pattern)
                    if concrete_topics:
                        self._attach_concrete_topics(pattern, concrete_topics)
            except Exception as error:
                print(f"[SUB] Reconciler error (continuing): {error}")

    def _list_topics_from_broker(self, pattern: str) -> list:
        """Call LIST_TOPICS on brokers and return resolved concrete topics for pattern."""
        message = protocol.encode_list_topics(pattern)

        for host, port in self._get_broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as sock:
                    sock.sendall(message.encode())
                    line = protocol.recv_line(sock)

                if line is None:
                    continue

                msg_type, payload = protocol.decode_message(line)

                if msg_type == "NACK":
                    leader_port = payload.get("leader_port", -1)
                    if leader_port > 0:
                        with self._leader_lock:
                            self._leader_port = leader_port
                    continue

                if msg_type == "TOPICS":
                    with self._leader_lock:
                        self._leader_port = port
                    return payload.get("topics", [])

                print(f"[SUB] Unexpected LIST_TOPICS response: {msg_type}")
                return []

            except (ConnectionRefusedError, socket.timeout, OSError) as error:
                print(f"[SUB] LIST_TOPICS cannot reach {host}:{port} - {error}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None

        return []

    def _start_poller(self, topic: str):
        """Start one poller thread for topic if no active poller exists."""
        if topic in self._poller_threads:
            thread = self._poller_threads[topic]
            if thread.is_alive():
                return

        thread = threading.Thread(
            target=self._poll_loop,
            args=(topic,),
            daemon=True,
            name=f"poller-{topic}",
        )
        self._poller_threads[topic] = thread
        thread.start()
        print(f"[SUB] Started poller thread for '{topic}'")

    def _poll_loop(self, topic: str):
        """Poll one concrete topic at the configured interval until unsubscribed."""
        print(f"[SUB] Poller active for '{topic}'")
        while self.log_mgr.is_subscribed(topic):
            try:
                self._fetch_new_entries(topic)
            except Exception as error:
                print(f"[SUB] Poller error for '{topic}': {error}")
            time.sleep(config.POLL_INTERVAL)

        self._poller_threads.pop(topic, None)
        print(f"[SUB] Poller stopped for '{topic}'")

    def _fetch_new_entries(self, topic: str):
        """Fetch and cache new bytes for a concrete topic using SUBSCRIBE protocol."""
        byte_offset = self.log_mgr.get_byte_offset(topic)
        message = protocol.encode_subscribe(topic, byte_offset)

        for host, port in self._get_broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as sock:
                    sock.sendall(message.encode())

                    header_line = protocol.recv_line(sock)
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
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        raw_content += chunk

                    with self._leader_lock:
                        self._leader_port = port

                    self.log_mgr.update_offset(topic, new_offset)
                    self.log_mgr.add_raw_bytes(topic, raw_content)

                    if raw_content:
                        lines = [line for line in raw_content.decode("utf-8", errors="replace").splitlines() if line.strip()]
                        print(f"[SUB] Received {len(lines)} new entries for '{topic}' (offset {byte_offset} -> {new_offset})")
                        for line in lines:
                            print(f"       {line}")
                    else:
                        print(f"[SUB] Poll '{topic}' has no new data (offset={byte_offset})")

                    return

            except (ConnectionRefusedError, socket.timeout, OSError) as error:
                print(f"[SUB] Cannot reach broker {host}:{port} - {error}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None

    def _get_broker_candidates(self) -> list:
        """Return candidate brokers with known leader first."""
        with self._leader_lock:
            leader_port = self._leader_port

        candidates = []
        if leader_port:
            leader_host = self._port_to_host(leader_port)
            candidates.append((leader_host, leader_port))

        for broker in config.BROKERS:
            if broker["port"] != leader_port:
                candidates.append((broker["host"], broker["port"]))

        return candidates

    def _port_to_host(self, port: int) -> str:
        """Resolve host from broker configuration by port value."""
        for broker in config.BROKERS:
            if broker["port"] == port:
                return broker["host"]
        return config.BROKERS[0]["host"]

    def run(self):
        """Start the HTTP server and block the current thread."""
        print(f"[SUB] HTTP API running at http://127.0.0.1:{self.http_port}")
        print(f"[SUB] Subscribe examples: /subscribe?topic=brave.server0, /subscribe?topic=server0, /subscribe?topic=brave")
        self.app.run(host="0.0.0.0", port=self.http_port, debug=False, use_reloader=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSMS Subscriber HTTP API for reading metrics.")
    parser.add_argument(
        "--port",
        type=int,
        default=config.SUBSCRIBER_HTTP_PORT,
        help=f"HTTP port for the REST API (default: {config.SUBSCRIBER_HTTP_PORT})",
    )
    args = parser.parse_args()

    subscriber = Subscriber(http_port=args.port)
    subscriber.run()
