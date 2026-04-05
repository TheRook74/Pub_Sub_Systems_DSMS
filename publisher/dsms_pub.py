# =============================================================================
# publisher/dsms_pub.py  —  DSMS Publisher Application (DSMS_PUB)
#
# This is the publisher side of the pub-sub system.
# Think of DSMS_PUB as a monitoring agent that runs on a server machine.
#
# STARTUP SEQUENCE:
#   1. Read apps.json to get: server_id + list of app names to monitor.
#   2. Build topic names: "app_name.server_id"
#      e.g.  apps=["chrome","python"], server_id="server1"
#             → topics = ["chrome.server1", "python.server1"]
#   3. For each topic, send CREATE_TOPIC to the broker cluster.
#      Retry until success (the cluster might be electing a leader at startup).
#   4. Enter the monitoring loop:
#      every PUBLISH_INTERVAL seconds:
#        - collect CPU/memory for each app using psutil
#        - send METRIC messages to the broker leader
#        - if a broker replies NACK, update the cached leader and retry
#        - if a broker is unreachable, try the next broker in the list
#
# HOW LEADER DISCOVERY WORKS:
#   The publisher doesn't know which broker is the leader at startup.
#   It tries each broker in turn:
#     - If the broker is the leader: it processes the request and responds ACK.
#     - If the broker is a follower: it responds NACK <leader_port>.
#       The publisher records this port and from now on sends directly to
#       the leader, skipping the extra round-trip.
#   If the leader crashes, the publisher gets NACK or a connection error,
#   and starts scanning brokers again to find the new leader.
# =============================================================================

import socket
import json
import time
import sys
import os

# Allow imports from the parent 'dsms' package regardless of working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol
from publisher.metrics import get_metrics_for_app, get_system_metrics


# =============================================================================
# Publisher class
# =============================================================================

class Publisher:
    """
    DSMS_PUB: reads apps.json, creates topics, and continuously publishes
    CPU/memory metrics to the broker cluster.
    """

    def __init__(self, apps_json_path: str):
        # ---- Load configuration ----
        with open(apps_json_path, "r") as f:
            cfg = json.load(f)

        self.server_id = cfg["server_id"]   # e.g. "server1"
        self.apps      = cfg["apps"]        # e.g. ["chrome", "python"]

        # Build topic names: "chrome.server1", "python.server1", etc.
        self.topics = [f"{app}.{self.server_id}" for app in self.apps]

        # Cache the known leader port for each topic.
        # Starts as None (unknown); filled in after the first successful contact.
        self.topic_leader: dict = {topic: None for topic in self.topics}

        print(f"[PUB] Server ID   : {self.server_id}")
        print(f"[PUB] Apps        : {self.apps}")
        print(f"[PUB] Topics      : {self.topics}")

    # =========================================================================
    # Public API
    # =========================================================================

    def run(self):
        """
        Full publisher lifecycle:
          1. Create all topics on the broker cluster.
          2. Enter the metric publishing loop.
        """
        print("[PUB] Creating topics on broker cluster...")
        self._create_all_topics()
        print("[PUB] All topics created.  Starting metric loop...")
        self._publish_loop()

    # =========================================================================
    # Step 1: Topic Creation
    # =========================================================================

    def _create_all_topics(self):
        """
        Send CREATE_TOPIC for every topic.
        Retries until success or MAX_RETRIES exceeded.
        If a topic fails after all retries, the publisher exits — there's no
        point sending metrics for a topic that doesn't exist on the cluster.
        """
        for topic in self.topics:
            success = False
            for attempt in range(1, config.MAX_RETRIES + 1):
                print(f"[PUB] Creating topic '{topic}' (attempt {attempt}/{config.MAX_RETRIES})")
                response = self._send_request(protocol.encode_create_topic(topic), topic)
                if response == "ACK":
                    print(f"[PUB] Topic '{topic}' created successfully.")
                    success = True
                    break
                elif response is None:
                    print(f"[PUB] No response. Retrying in {config.RETRY_DELAY}s...")
                    time.sleep(config.RETRY_DELAY)
                else:
                    print(f"[PUB] Unexpected response: {response}. Retrying...")
                    time.sleep(config.RETRY_DELAY)

            if not success:
                print(f"[PUB] FATAL: Could not create topic '{topic}' after {config.MAX_RETRIES} attempts. Exiting.")
                sys.exit(1)

    # =========================================================================
    # Step 2: Metric Publishing Loop
    # =========================================================================

    def _publish_loop(self):
        """
        Runs forever.  Every PUBLISH_INTERVAL seconds, collects metrics for
        all configured apps and sends them to the broker cluster.
        """
        while True:
            for app, topic in zip(self.apps, self.topics):
                # Collect metrics from OS using psutil
                data = get_metrics_for_app(app)
                if data is None:
                    # App not running — use system-wide metrics as a fallback
                    data = get_system_metrics()
                    print(f"[PUB] '{app}' not found in processes; using system metrics.")

                print(f"[PUB] Sending METRIC | topic={topic} | {data}")
                self._send_metric_with_retry(topic, data)

            # Wait before collecting again
            time.sleep(config.PUBLISH_INTERVAL)

    def _send_metric_with_retry(self, topic: str, data: str):
        """
        Send a METRIC message for 'topic'.  Handle NACK (wrong node) and
        connection failures by trying other brokers.
        """
        for attempt in range(config.MAX_RETRIES):
            response = self._send_request(protocol.encode_metric(topic, data), topic)
            if response == "ACK":
                return  # successfully committed
            elif response is None:
                # Connection failed — clear leader cache and try next broker
                self.topic_leader[topic] = None
                time.sleep(0.5)
            else:
                # Unexpected response (e.g. topic not found error)
                print(f"[PUB] Unexpected response for METRIC: {response}")
                time.sleep(0.5)

        print(f"[PUB] WARNING: Could not commit metric for '{topic}' after {config.MAX_RETRIES} attempts.")

    # =========================================================================
    # Core Send Logic
    # =========================================================================

    def _send_request(self, message: str, topic: str) -> str:
        """
        Send a text message to the broker and return the response string.

        Leader discovery logic:
          - If we know the leader port for this topic, connect there directly.
          - Otherwise, try each broker in config.BROKERS in round-robin order.
          - On NACK: record the redirected leader port, reconnect there, retry.
          - On connection failure: mark that port as dead for this call and skip it.

        Returns "ACK" on success, or None on total failure.

        KEY TIMEOUT RULE:
            Connection timeout = 8 seconds, which is greater than the broker's
            wait_for_commit timeout (6 seconds).  This ensures we always wait
            long enough to receive the ACK even if the commit takes a few seconds
            (common right after a leader failover when the RAFT pipeline is
            re-establishing).  Using 3s previously caused the publisher to
            time-out and declare failure even though the broker had committed.
        """
        CONN_TIMEOUT = 8.0  # must be > broker's wait_for_commit timeout (6.0s)

        # Ports that have already failed with a connection error in this call.
        # We skip them in the NACK-redirect step so we don't waste time on a
        # broker we already know is down.
        dead_ports = set()

        candidates = self._get_broker_candidates(topic)

        for host, port in candidates:
            if port in dead_ports:
                continue

            try:
                with socket.create_connection((host, port), timeout=CONN_TIMEOUT) as s:
                    s.sendall(message.encode())
                    response_line = protocol.recv_line(s)

                if response_line is None:
                    continue

                msg_type, payload = protocol.decode_message(response_line)

                if msg_type == "ACK":
                    # Cache this port as the known leader for this topic
                    self.topic_leader[topic] = port
                    return "ACK"

                elif msg_type == "NACK":
                    leader_port = payload.get("leader_port", -1)
                    if leader_port > 0 and leader_port not in dead_ports:
                        print(f"[PUB] NACK from port {port} → leader is port {leader_port}")
                        self.topic_leader[topic] = leader_port
                        leader_host = self._port_to_host(leader_port)
                        try:
                            # Retry directly with the leader using the full timeout
                            with socket.create_connection(
                                    (leader_host, leader_port),
                                    timeout=CONN_TIMEOUT) as s2:
                                s2.sendall(message.encode())
                                resp2 = protocol.recv_line(s2)
                            if resp2:
                                t2, _ = protocol.decode_message(resp2)
                                if t2 == "ACK":
                                    return "ACK"
                        except (ConnectionRefusedError, socket.timeout, OSError) as e2:
                            print(f"[PUB] Cannot reach redirected leader "
                                  f"{leader_host}:{leader_port} — {e2}")
                            dead_ports.add(leader_port)
                            self.topic_leader[topic] = None
                    else:
                        # No leader elected yet (election in progress)
                        print("[PUB] NACK: no leader elected yet. Waiting...")
                        time.sleep(config.RETRY_DELAY)

                else:
                    return response_line.strip() if response_line else None

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[PUB] Cannot reach broker at {host}:{port} — {e}")
                dead_ports.add(port)
                if self.topic_leader.get(topic) == port:
                    self.topic_leader[topic] = None

        return None  # all brokers failed

    # =========================================================================
    # Helpers
    # =========================================================================

    def _get_broker_candidates(self, topic: str) -> list:
        """
        Return a list of (host, port) tuples to try, leader first.
        """
        known_leader_port = self.topic_leader.get(topic)
        candidates = []
        if known_leader_port:
            leader_host = self._port_to_host(known_leader_port)
            candidates.append((leader_host, known_leader_port))
        for b in config.BROKERS:
            if b["port"] != known_leader_port:
                candidates.append((b["host"], b["port"]))
        return candidates

    def _port_to_host(self, port: int) -> str:
        """Look up the host for a broker port from config."""
        for b in config.BROKERS:
            if b["port"] == port:
                return b["host"]
        # Should never happen — all ports must be listed in config.BROKERS
        return config.BROKERS[0]["host"]


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    # Resolve the path to apps.json relative to this script's directory
    default_apps_json = os.path.join(os.path.dirname(__file__), "apps.json")

    import argparse
    parser = argparse.ArgumentParser(description="DSMS Publisher — monitors apps and publishes metrics.")
    parser.add_argument(
        "--apps", default=default_apps_json,
        help="Path to apps.json configuration file (default: publisher/apps.json)",
    )
    args = parser.parse_args()

    pub = Publisher(apps_json_path=args.apps)
    pub.run()
