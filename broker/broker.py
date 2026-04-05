# =============================================================================
# broker/broker.py
#
# The main broker process.  Each broker node in the cluster runs this file.
#
# What this file does:
#   1. Starts a TCP server on the node's configured port (from config.BROKERS).
#   2. Accepts incoming TCP connections — both from clients (publishers,
#      subscribers) and from peer brokers (RAFT messages).
#   3. For each connection, spawns a handler thread that reads the message,
#      routes it to the right handler, and sends a response.
#   4. Initialises the LogStore (disk storage) and RaftNode (consensus).
#
# Message routing:
#   CREATE_TOPIC  → handle_create_topic()   (client → leader → RAFT)
#   METRIC        → handle_metric()          (client → leader → RAFT)
#   SUBSCRIBE     → handle_subscribe()       (client reads from log file)
#   VOTE_REQUEST  → raft.handle_vote_request()
#   VOTE_GRANTED  → raft.handle_vote_granted()
#   VOTE_DENIED   → raft.handle_vote_denied()
#   REPLICATE     → raft.handle_replicate()
#   REPLICATE_ACK → raft.handle_replicate_ack()
#
# How to run:
#   python broker.py --id 0      (starts broker node 0 on port 5000)
#   python broker.py --id 1      (starts broker node 1 on port 5001)
#   python broker.py --id 2      (starts broker node 2 on port 5002)
# =============================================================================

import socket
import threading
import argparse
import sys
import os

# Allow imports from the parent 'dsms' package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol
from broker.log_store import LogStore
from broker.raft import RaftNode


class Broker:
    """
    One broker node in the DSMS cluster.
    Combines a TCP server with a RAFT consensus node and a LogStore.
    """

    def __init__(self, node_id: int):
        """
        node_id: integer matching an entry in config.BROKERS.
        """
        self.node_id = node_id

        # Look up our host and port from the global cluster config
        node_cfg = next(b for b in config.BROKERS if b["id"] == node_id)
        self.host = node_cfg["host"]
        self.port = node_cfg["port"]

        # ---- Storage ----
        # Each broker stores its own copy of all topic logs under a unique dir.
        data_dir = os.path.join(os.path.dirname(__file__),
                                "..", "broker_data", f"node{node_id}")
        self.log_store = LogStore(base_dir=data_dir)

        # ---- RAFT ----
        # Pass self._send_to_peer as the network function so RAFT can
        # contact other brokers without knowing about sockets directly.
        self.raft = RaftNode(
            node_id   = node_id,
            log_store = self.log_store,
            send_to_peer_fn = self._send_to_peer,
        )

        # ---- TCP Server ----
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR lets us restart the broker quickly without "Address in use" errors
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(50)  # up to 50 queued connections
        print(f"[Broker {node_id}] Listening on {self.host}:{self.port}")

    # =========================================================================
    # Main Accept Loop
    # =========================================================================

    def serve_forever(self):
        """
        Blocks and accepts incoming connections indefinitely.
        Each connection is handled in a separate daemon thread so many
        clients can be served concurrently.
        """
        print(f"[Broker {self.node_id}] Ready to accept connections.")
        while True:
            try:
                conn, addr = self.server_sock.accept()
                # Spawn a thread per connection; daemon=True means it dies
                # automatically when the main process exits.
                t = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True,
                )
                t.start()
            except KeyboardInterrupt:
                print(f"\n[Broker {self.node_id}] Shutting down.")
                break
            except Exception as e:
                print(f"[Broker {self.node_id}] Accept error: {e}")

    # =========================================================================
    # Connection Handler
    # =========================================================================

    def _handle_connection(self, conn: socket.socket, addr):
        """
        Called in its own thread for every incoming TCP connection.
        Reads exactly ONE message (one line), dispatches it, and responds.
        """
        try:
            conn.settimeout(5.0)    # don't block forever waiting for data
            line = protocol.recv_line(conn)
            if not line:
                return

            msg_type, payload = protocol.decode_message(line)
            if msg_type is None:
                return

            # ---- Route message to correct handler ----

            if msg_type == "CREATE_TOPIC":
                self._handle_create_topic(conn, payload["topic"])

            elif msg_type == "METRIC":
                self._handle_metric(conn, payload["topic"], payload["data"])

            elif msg_type == "SUBSCRIBE":
                self._handle_subscribe(conn, payload["topic"], payload["offset"])

            # ---- RAFT internal messages (no response sent back here;
            #      RAFT handlers enqueue responses via _queue_send) ----

            elif msg_type == "VOTE_REQUEST":
                self.raft.handle_vote_request(
                    payload["term"], payload["candidate_id"],
                    payload["last_log_index"], payload["last_log_term"])

            elif msg_type == "VOTE_GRANTED":
                self.raft.handle_vote_granted(payload["term"], payload["voter_id"])

            elif msg_type == "VOTE_DENIED":
                self.raft.handle_vote_denied(payload["term"])

            elif msg_type == "REPLICATE":
                self.raft.handle_replicate(
                    payload["term"], payload["leader_id"],
                    payload["prev_log_index"], payload["prev_log_term"],
                    payload["entries"], payload["commit_index"])

            elif msg_type == "REPLICATE_ACK":
                self.raft.handle_replicate_ack(
                    payload["term"], payload["follower_id"],
                    payload["match_index"], payload["success"])

            else:
                conn.sendall(f"ERROR unknown message type: {msg_type}\n".encode())

        except Exception as e:
            print(f"[Broker {self.node_id}] Error handling connection from {addr}: {e}")
        finally:
            conn.close()

    # =========================================================================
    # Client Request Handlers
    # =========================================================================

    def _handle_create_topic(self, conn: socket.socket, topic: str):
        """
        Process a CREATE_TOPIC request from a publisher.

        Flow:
          1. If we are NOT the leader → send NACK with leader's port.
          2. If we ARE the leader:
             a. Check if topic already exists (fast path: just ACK).
             b. Otherwise: propose through RAFT, wait for commit, send ACK.
             c. If RAFT commit times out → send ERROR.
        """
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        # Fast path: topic already committed by a previous call
        if self.log_store.topic_exists(topic):
            conn.sendall(protocol.encode_ack().encode())
            return

        # Propose through RAFT so ALL brokers create the topic atomically
        idx = self.raft.propose("CREATE_TOPIC", topic)
        if idx < 0:
            # We lost leadership between the is_leader() check and propose()
            conn.sendall(protocol.encode_nack(self.raft.get_leader_port()).encode())
            return

        # Wait for the entry to be committed (majority of brokers acknowledged)
        committed = self.raft.wait_for_commit(idx)
        if committed:
            print(f"[Broker {self.node_id}] CREATE_TOPIC '{topic}' committed.")
            conn.sendall(protocol.encode_ack().encode())
        else:
            conn.sendall(b"ERROR topic creation timed out\n")

    def _handle_metric(self, conn: socket.socket, topic: str, data: str):
        """
        Process a METRIC message from a publisher.

        Flow:
          1. If we are NOT the leader → send NACK.
          2. If topic doesn't exist yet → send ERROR (publisher must CREATE_TOPIC first).
          3. Propose through RAFT, wait for commit, send ACK.
        """
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        if not self.log_store.topic_exists(topic):
            conn.sendall(f"ERROR topic '{topic}' does not exist\n".encode())
            return

        idx = self.raft.propose("METRIC", topic, data)
        if idx < 0:
            conn.sendall(protocol.encode_nack(self.raft.get_leader_port()).encode())
            return

        committed = self.raft.wait_for_commit(idx)
        if committed:
            conn.sendall(protocol.encode_ack().encode())
        else:
            conn.sendall(b"ERROR metric commit timed out\n")

    def _handle_subscribe(self, conn: socket.socket, topic: str, byte_offset: int):
        """
        Process a SUBSCRIBE request from a subscriber.

        The subscriber tells us where it left off (byte_offset).
        We read from that position in the log file and return any new entries.

        Response format (two parts):
          Part 1: "DATA <new_offset>\n"   (one text line)
          Part 2: <raw log bytes>          (zero or more JSON lines)

        If this broker is NOT the leader, we still serve reads.
        (In this prototype, we only read from the leader for simplicity
         so the subscriber always sees committed data.)
        """
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        content_bytes, new_offset = self.log_store.read(topic, byte_offset)

        # Send header line first, then the raw content bytes
        conn.sendall(protocol.encode_data_header(new_offset))
        if content_bytes:
            conn.sendall(content_bytes)

    # =========================================================================
    # Inter-broker Communication (used by RaftNode via send_to_peer_fn)
    # =========================================================================

    def _send_to_peer(self, peer_id: int, message: str):
        """
        Open a TCP connection to peer broker, send message, close.
        This is a fire-and-forget approach: we don't wait for a reply here.
        The peer broker's handle_connection() will process the message and
        send any response as a new connection back to us.
        """
        peer_cfg = next((b for b in config.BROKERS if b["id"] == peer_id), None)
        if peer_cfg is None:
            return
        try:
            with socket.create_connection(
                    (peer_cfg["host"], peer_cfg["port"]), timeout=1.0) as s:
                s.sendall(message.encode())
        except (ConnectionRefusedError, socket.timeout, OSError):
            # Peer is down — RAFT is designed to handle this gracefully
            pass


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    # Parse which node ID this broker instance should be
    parser = argparse.ArgumentParser(description="Start a DSMS broker node.")
    parser.add_argument(
        "--id", type=int, required=True,
        help="Broker node ID (must match an entry in common/config.py BROKERS list)",
    )
    args = parser.parse_args()

    if args.id not in [b["id"] for b in config.BROKERS]:
        print(f"ERROR: node id {args.id} not found in config.BROKERS")
        sys.exit(1)

    broker = Broker(node_id=args.id)
    broker.serve_forever()
