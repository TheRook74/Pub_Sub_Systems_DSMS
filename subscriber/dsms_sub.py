# =============================================================================
# subscriber/dsms_sub.py  -  DSMS Subscriber Application (DSMS_SUB)
#
# Design (new):
#   Every POST /subscribe?topic=<pattern> spawns ONE dedicated Subscription:
#     * its own background thread
#     * its own output file  (subscriber_output/<pattern>_<ts>_sub<id>.log)
#     * its own {concrete topic -> byte offset} dict, each offset starting
#       at 0 so the file captures the full history present on the broker
#       at the time of subscribe AND every new entry from then onwards
#     * its own stop flag, set by DELETE /subscribe
#
#   Subscriptions never share state.  Two POSTs for the same pattern
#   produce two independent threads, two independent files, and two
#   independent offset maps.  Because offsets start at 0, the two files
#   are byte-for-byte equivalent modulo the moments they were created.
#
# HTTP API:
#   POST   /subscribe?topic=<pattern>      create a subscription
#   DELETE /subscribe?id=<id>              stop one subscription
#   DELETE /subscribe?topic=<pattern>      stop every subscription for a pattern
#   GET    /status                         list all live subscriptions
#
# Patterns supported on /subscribe:
#   brave.server0   (exact concrete topic)
#   brave           (app prefix   -> every brave.* topic)
#   server0         (server suffix-> every *.server0 topic)
#   The broker resolves them via LIST_TOPICS; new concrete topics that
#   show up after the subscription is created are picked up on the next
#   poll cycle and also start at offset 0 inside that file.
# =============================================================================

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, request

# Allow imports from the parent 'dsms' package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol


# =============================================================================
# Output directory + filename helpers
# =============================================================================

OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "subscriber_output")
)


def _sanitize_pattern(pattern: str) -> str:
    """Turn a pattern into a safe filesystem fragment."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", pattern)


def _build_output_filename(sub_id: int, pattern: str, created_at: datetime) -> str:
    """
    Produce a unique filename per subscription.
    Example: brave.server0_2026-04-04_12-34-56_sub3.log
    """
    ts = created_at.strftime("%Y-%m-%d_%H-%M-%S")
    return f"{_sanitize_pattern(pattern)}_{ts}_sub{sub_id}.log"


# =============================================================================
# Subscription: one thread, one file, one offsets map
# =============================================================================

class Subscription:
    """
    A single /subscribe request incarnated as a background worker.

    Each instance is fully self-contained: stopping one does not affect any
    other subscription, even another one with the exact same pattern.
    """

    def __init__(self, sub_id: int, pattern: str, subscriber: "Subscriber"):
        self.id = sub_id
        self.pattern = pattern
        self.created_at = datetime.now()
        self._subscriber = subscriber   # back-ref used only for shared network helpers

        # Offsets: concrete_topic -> next byte to fetch from broker.
        # All new topics enter with 0 so the file captures full history.
        self._offsets: dict = {}
        self._offsets_lock = threading.Lock()

        # Open the output file right now so /status and `ls` see it
        # immediately after POST returns, even before the first poll runs.
        self.file_path = os.path.join(
            OUTPUT_DIR,
            _build_output_filename(self.id, self.pattern, self.created_at),
        )
        # "wb" truncates — each subscribe is a fresh file.  Subsequent
        # writes are always append-from-end in the same handle, so the
        # file only ever grows, no row is ever rewritten.
        self._fh = open(self.file_path, "wb")
        self._file_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"sub{self.id}-{pattern}",
        )
        self._thread.start()

    # ------------------------------------------------------------------ control

    def stop(self):
        """Signal the thread to exit.  Safe to call more than once."""
        self._stop_event.set()

    def alive(self) -> bool:
        return self._thread.is_alive()

    # ----------------------------------------------------------------- status

    def snapshot(self) -> dict:
        """Return a JSON-safe description used by /status."""
        with self._offsets_lock:
            offsets = dict(self._offsets)
        return {
            "id":         self.id,
            "pattern":    self.pattern,
            "created_at": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file":       self.file_path,
            "alive":      self.alive(),
            "topics":     offsets,   # {concrete_topic: last_known_offset}
        }

    # --------------------------------------------------------------- main loop

    def _run(self):
        """
        Loop body:
          1. Expand the pattern via LIST_TOPICS (every cycle -> picks up new
             concrete topics that were created after we subscribed).
          2. Register every newly-discovered topic at offset 0.
          3. Poll each known topic at its current offset, append new rows to
             the file, update the offset.
          4. Sleep POLL_INTERVAL (abortable via _stop_event).
        """
        print(f"[SUB#{self.id}] Started | pattern='{self.pattern}' "
              f"| file='{self.file_path}'")

        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except Exception as e:
                # Don't let a transient network / parse error kill the thread.
                print(f"[SUB#{self.id}] Poll error (continuing): {e}")

            # _stop_event.wait returns True if we were told to stop.
            if self._stop_event.wait(timeout=config.POLL_INTERVAL):
                break

        # Clean shutdown: flush + close the file so partial buffers don't
        # get lost on exit.
        try:
            with self._file_lock:
                self._fh.flush()
                self._fh.close()
        except OSError:
            pass
        print(f"[SUB#{self.id}] Stopped")

    def _poll_once(self):
        """One iteration of the loop: discover + poll + write."""
        # 1. Discover concrete topics that currently match the pattern.
        concrete = self._subscriber.list_topics_from_broker(self.pattern)

        with self._offsets_lock:
            # 2. Bring any new topics into our tracking map at offset 0.
            for t in concrete:
                if t not in self._offsets:
                    self._offsets[t] = 0
                    print(f"[SUB#{self.id}] Tracking new topic '{t}' from offset 0")
            # Snapshot what to poll this cycle.
            topics_to_poll = list(self._offsets.items())

        # 3. Pull new bytes for each topic and append to our file.
        for topic, offset in topics_to_poll:
            if self._stop_event.is_set():
                return
            self._fetch_topic(topic, offset)

    def _fetch_topic(self, topic: str, start_offset: int):
        """
        SUBSCRIBE a single concrete topic, write whatever new bytes the
        broker returned to this subscription's file, then advance the
        offset so the next cycle picks up from where this one ended.
        """
        result = self._subscriber.fetch_topic_bytes(topic, start_offset)
        if result is None:
            return  # broker unreachable this cycle; will retry
        new_offset, raw_content = result

        if raw_content:
            self._write_raw_to_file(topic, raw_content)

        with self._offsets_lock:
            self._offsets[topic] = new_offset

    def _write_raw_to_file(self, topic: str, raw_bytes: bytes):
        """
        Tag each JSON line with its source topic (needed so aggregated
        files like server0_*.log remain unambiguous) and append the
        block as one write.
        """
        tagged_lines = []
        for line in raw_bytes.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            tagged = {"topic": topic, **entry}
            tagged_lines.append(json.dumps(tagged))

        if not tagged_lines:
            return
        payload = ("\n".join(tagged_lines) + "\n").encode("utf-8")

        with self._file_lock:
            if self._fh.closed:
                return
            try:
                self._fh.write(payload)
                self._fh.flush()
            except OSError as e:
                print(f"[SUB#{self.id}] Write error: {e}")


# =============================================================================
# Subscriber: HTTP layer + shared leader discovery + subscription registry
# =============================================================================

class Subscriber:
    """
    HTTP frontend that owns all Subscription instances.

    Only global state kept here is:
      * the registry of live subscriptions (by id)
      * the shared "last known leader port" so that every Subscription
        benefits when any one of them discovers the current RAFT leader
    """

    def __init__(self, http_port: int = config.SUBSCRIBER_HTTP_PORT):
        self.http_port = http_port

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        self._subs: dict = {}                 # sub_id -> Subscription
        self._subs_lock = threading.Lock()
        self._next_sub_id = 1

        self._leader_port = None
        self._leader_lock = threading.Lock()

        self.app = Flask(__name__)
        self._register_routes()

    # ------------------------------------------------------------------ routes

    def _register_routes(self):
        app = self.app

        # POST /subscribe?topic=<pattern>
        @app.route("/subscribe", methods=["POST"])
        def subscribe():
            pattern = request.args.get("topic", "").strip()
            if not pattern:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            sub = self._spawn_subscription(pattern)
            return jsonify({
                "status":          "subscribed",
                "subscription_id": sub.id,
                "pattern":         sub.pattern,
                "file":            sub.file_path,
                "created_at":      sub.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }), 200

        # DELETE /subscribe?id=<id>  OR  DELETE /subscribe?topic=<pattern>
        @app.route("/subscribe", methods=["DELETE"])
        def unsubscribe():
            raw_id = request.args.get("id", "").strip()
            pattern = request.args.get("topic", "").strip()

            if not raw_id and not pattern:
                return jsonify({
                    "error": "Provide either 'id' (one subscription) "
                             "or 'topic' (every subscription for that pattern)",
                }), 400

            stopped = []

            if raw_id:
                try:
                    sid = int(raw_id)
                except ValueError:
                    return jsonify({"error": f"Invalid id '{raw_id}'"}), 400
                with self._subs_lock:
                    sub = self._subs.pop(sid, None)
                if sub is None:
                    return jsonify({"error": f"No subscription with id={sid}"}), 404
                sub.stop()
                stopped.append(sub.id)
            else:
                with self._subs_lock:
                    ids = [sid for sid, s in self._subs.items() if s.pattern == pattern]
                    subs = [self._subs.pop(sid) for sid in ids]
                for s in subs:
                    s.stop()
                    stopped.append(s.id)

            return jsonify({
                "status":  "unsubscribed",
                "stopped": stopped,
            }), 200

        # GET /status
        @app.route("/status", methods=["GET"])
        def status():
            with self._subs_lock:
                subs = [s.snapshot() for s in self._subs.values()]
            return jsonify({
                "leader_port":   self._leader_port,
                "subscriptions": subs,
            }), 200

    # ------------------------------------------------------- subscription mgmt

    def _spawn_subscription(self, pattern: str) -> Subscription:
        """Allocate an id and launch a new Subscription thread."""
        with self._subs_lock:
            sub_id = self._next_sub_id
            self._next_sub_id += 1

        # Subscription construction opens the file and starts the thread.
        sub = Subscription(sub_id, pattern, self)

        with self._subs_lock:
            self._subs[sub_id] = sub

        print(f"[SUB] Created subscription #{sub.id} pattern='{pattern}' "
              f"file='{sub.file_path}'")
        return sub

    # ======================================================================
    # Broker network helpers (shared across all Subscription instances)
    # ======================================================================

    def list_topics_from_broker(self, pattern: str) -> list:
        """
        Ask the RAFT leader for every concrete topic matching a pattern.
        Returns an empty list if every broker is unreachable or nothing
        matches.  Updates _leader_port as a side effect so subsequent
        calls go directly to the leader.
        """
        message = protocol.encode_list_topics(pattern)
        for host, port in self._broker_candidates():
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
                    continue  # retry at the hinted leader

                if msg_type == "TOPICS":
                    with self._leader_lock:
                        self._leader_port = port
                    return payload.get("topics", [])

                print(f"[SUB] Unexpected LIST_TOPICS response: {msg_type}")
                return []
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] LIST_TOPICS cannot reach {host}:{port} - {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None
        return []

    def fetch_topic_bytes(self, topic: str, byte_offset: int):
        """
        Send SUBSCRIBE <topic> <byte_offset> and return (new_offset, raw_bytes).
        Returns None if no broker could serve the request this cycle.
        """
        message = protocol.encode_subscribe(topic, byte_offset)
        for host, port in self._broker_candidates():
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
                        continue  # retry at hinted leader

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
                    return new_offset, raw_content
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] Fetch cannot reach {host}:{port} - {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None
        return None

    def _broker_candidates(self) -> list:
        """Known leader first, then the rest of the brokers as fallback."""
        with self._leader_lock:
            lp = self._leader_port

        ordered = []
        if lp:
            host = self._port_to_host(lp)
            ordered.append((host, lp))
        for b in config.BROKERS:
            if b["port"] != lp:
                ordered.append((b["host"], b["port"]))
        return ordered

    def _port_to_host(self, port: int) -> str:
        for b in config.BROKERS:
            if b["port"] == port:
                return b["host"]
        return config.BROKERS[0]["host"]

    # --------------------------------------------------------------------- run

    def run(self):
        """Start the Flask HTTP server (blocking)."""
        print(f"[SUB] HTTP API running at http://127.0.0.1:{self.http_port}")
        print(f"[SUB] Output directory: {OUTPUT_DIR}")
        print(f"[SUB] Subscribe (each call spawns its own thread + file):")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave.server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave'")
        print(f"[SUB] Unsubscribe by id or by pattern:")
        print(f"       curl -X DELETE 'http://localhost:{self.http_port}/subscribe?id=1'")
        print(f"       curl -X DELETE 'http://localhost:{self.http_port}/subscribe?topic=server0'")
        self.app.run(host="0.0.0.0", port=self.http_port,
                     debug=False, use_reloader=False)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSMS Subscriber - per-subscribe thread + file.")
    parser.add_argument(
        "--port", type=int, default=config.SUBSCRIBER_HTTP_PORT,
        help=f"HTTP port for the REST API (default: {config.SUBSCRIBER_HTTP_PORT})",
    )
    args = parser.parse_args()

    sub = Subscriber(http_port=args.port)
    sub.run()
