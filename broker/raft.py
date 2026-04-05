# =============================================================================
# broker/raft.py
#
# Implementation of the RAFT consensus algorithm.
#
# WHY RAFT?
#   Our broker cluster has multiple nodes.  Without coordination, each node
#   might store different data in a different order — that's inconsistent.
#   RAFT solves this by electing ONE leader who decides the order of all writes.
#   Every follower copies the leader's log exactly.  This way ALL nodes store
#   the SAME data in the SAME order, even if some nodes crash and restart.
#
# THE THREE STATES a node can be in:
#   FOLLOWER  – passive; follows the leader, rejects direct client writes
#   CANDIDATE – trying to become leader (this happens during an election)
#   LEADER    – handles all writes; sends heartbeats to keep followers awake
#
# KEY RAFT RULES (simplified):
#   1. If a follower doesn't hear from the leader within ELECTION_TIMEOUT,
#      it assumes the leader died and starts an election.
#   2. To win an election a candidate needs votes from MORE than half the nodes.
#   3. Each node votes for at most ONE candidate per term.
#   4. An entry in the RAFT log is "committed" once the leader has confirmation
#      from a majority that they stored it.  Only committed entries are applied
#      to the actual topic log files.
#   5. If any node sees a term number higher than its own, it immediately
#      reverts to FOLLOWER and updates its term.
#
# RAFT LOG vs TOPIC LOG:
#   - The RAFT log (self.raft_log) is an in-memory list of proposed operations.
#     It records every CREATE_TOPIC and METRIC command in order.
#   - The topic log (LogStore files) is the on-disk data that subscribers read.
#   - An entry moves from RAFT log → topic log only AFTER it is committed.
# =============================================================================

import threading
import random
import time
import queue
import sys
import os

# Allow imports from the parent 'dsms' package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol


class RaftNode:
    """
    RAFT consensus node.  One instance runs per broker process.
    Background threads handle the election timer, heartbeats, and log applying.
    """

    def __init__(self, node_id: int, log_store, send_to_peer_fn):
        """
        node_id        : integer ID of this broker (must match config.BROKERS)
        log_store      : LogStore instance – used when applying committed entries
        send_to_peer_fn: callable(peer_id, message_string) that delivers a RAFT
                         message to another broker.  Provided by broker.py.
        """
        self.node_id    = node_id
        self.log_store  = log_store
        self._send_raw  = send_to_peer_fn   # the actual network sender

        # IDs of all other nodes in the cluster
        self.peers        = [b["id"] for b in config.BROKERS if b["id"] != node_id]
        self.cluster_size = len(config.BROKERS)

        # ----------------------------------------------------------------
        # RAFT Persistent State
        # (In production these are saved to disk so they survive crashes.
        #  Here we keep them in memory for simplicity.)
        # ----------------------------------------------------------------
        self.current_term = 0       # latest election term this node has seen
        self.voted_for    = None    # which candidate we voted for in current_term

        # The RAFT log: a list of entry dicts, in order.
        # Entry format: {"index": int, "term": int,
        #                "type": "METRIC"|"CREATE_TOPIC",
        #                "topic": str, "data": str}
        self.raft_log = []

        # ----------------------------------------------------------------
        # RAFT Volatile State
        # ----------------------------------------------------------------
        self.commit_index = -1  # index of highest log entry known to be committed
        self.last_applied = -1  # index of highest log entry applied to log_store

        # ----------------------------------------------------------------
        # Leader-only State  (meaningful only when state == "LEADER")
        # ----------------------------------------------------------------
        # next_index[peer]  = the next log index to send to that follower
        # match_index[peer] = highest index confirmed to be on that follower
        self.next_index  = {}
        self.match_index = {}

        # ----------------------------------------------------------------
        # Node State
        # ----------------------------------------------------------------
        self.state      = "FOLLOWER"
        self.leader_id  = None         # ID of the current leader (or None)
        self.votes_received = set()    # used during candidate state

        # ----------------------------------------------------------------
        # Thread Safety
        # ----------------------------------------------------------------
        # ALL reads/writes to the above state must hold this lock.
        self._lock = threading.Lock()

        # ----------------------------------------------------------------
        # Outgoing message queue
        # A background sender thread dequeues (peer_id, message) pairs and
        # sends them.  This way we NEVER call the network while holding
        # self._lock, which would risk a deadlock.
        # ----------------------------------------------------------------
        self._send_queue = queue.Queue()
        threading.Thread(target=self._send_worker, daemon=True, name="raft-sender").start()

        # ----------------------------------------------------------------
        # Election Timer State
        # ----------------------------------------------------------------
        self._timer_deadline = 0.0   # epoch time at which election starts
        self._reset_timer_locked()   # set first random deadline

        # ----------------------------------------------------------------
        # Start background threads
        # ----------------------------------------------------------------
        threading.Thread(target=self._election_timer_loop,
                         daemon=True, name="raft-election-timer").start()
        threading.Thread(target=self._apply_loop,
                         daemon=True, name="raft-apply").start()

        print(f"[RAFT {self.node_id}] Started as FOLLOWER | term=0 | peers={self.peers}")

    # =========================================================================
    # Public API  (called by broker.py)
    # =========================================================================

    def is_leader(self) -> bool:
        """Return True if this node is currently the RAFT leader."""
        with self._lock:
            return self.state == "LEADER"

    def get_leader_port(self):
        """
        Return the TCP port of the current known leader, or -1 if unknown.
        The publisher/subscriber uses this to redirect its request.
        """
        with self._lock:
            if self.leader_id is None:
                return -1
            for b in config.BROKERS:
                if b["id"] == self.leader_id:
                    return b["port"]
            return -1

    def propose(self, entry_type: str, topic: str, data: str = "") -> int:
        """
        Submit a new operation to the RAFT log.  Only the leader can do this.

        entry_type : "METRIC" or "CREATE_TOPIC"
        topic      : topic name, e.g. "app1.server1"
        data       : metric data string (empty for CREATE_TOPIC)

        Returns the log index of the new entry (>= 0), or -1 if not leader.
        The caller should follow up with wait_for_commit(index) to know when
        the entry is actually committed.
        """
        with self._lock:
            if self.state != "LEADER":
                return -1

            new_index = len(self.raft_log)
            entry = {
                "index": new_index,
                "term":  self.current_term,
                "type":  entry_type,
                "topic": topic,
                "data":  data,
                # Timestamp recorded at proposal time on the LEADER.
                # All follower nodes will use this same timestamp when they
                # apply the entry to disk — this guarantees byte-identical
                # log files across the entire cluster.
                "ts":    time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            self.raft_log.append(entry)
            print(f"[RAFT {self.node_id}] Proposed [{entry_type}] '{topic}' at index {new_index}")

            # Queue replication to all followers immediately
            self._queue_replicate_all()
            return new_index

    def wait_for_commit(self, index: int, timeout: float = 6.0) -> bool:
        """
        Block (spin) until the entry at 'index' is committed or timeout expires.
        Returns True if committed in time, False on timeout.

        Called by broker.py after propose() to know when to send ACK to client.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self.commit_index >= index:
                    return True
            time.sleep(0.05)    # poll every 50 ms
        return False

    # =========================================================================
    # RAFT Message Handlers  (called by broker.py when a RAFT message arrives)
    # =========================================================================

    def handle_vote_request(self, term: int, candidate_id: int,
                            last_log_index: int, last_log_term: int):
        """
        Another node is asking for our vote in a new election.

        We grant the vote ONLY if all three conditions are true:
          (a) Candidate's term >= our current term
          (b) We haven't already voted for someone else this term
          (c) Candidate's log is at least as up-to-date as ours
              (prevents electing a node that is missing committed entries)
        """
        with self._lock:
            # Rule: if we see a higher term, update and revert to FOLLOWER
            if term > self.current_term:
                self._step_down(term)

            grant = False
            if (term == self.current_term
                    and (self.voted_for is None or self.voted_for == candidate_id)
                    and self._log_is_ok(last_log_index, last_log_term)):

                self.voted_for = candidate_id
                grant = True
                # Reset our timer so we don't immediately start our own election
                self._reset_timer_locked()
                print(f"[RAFT {self.node_id}] GRANTED vote to Node {candidate_id} (term {term})")
            else:
                reason = []
                if term < self.current_term:
                    reason.append("stale term")
                if self.voted_for not in (None, candidate_id):
                    reason.append(f"already voted for {self.voted_for}")
                if not self._log_is_ok(last_log_index, last_log_term):
                    reason.append("candidate log behind ours")
                print(f"[RAFT {self.node_id}] DENIED vote to Node {candidate_id}: {', '.join(reason)}")

            # Queue the response (send AFTER releasing lock via send worker)
            if grant:
                self._queue_send(candidate_id,
                    protocol.encode_vote_granted(self.current_term, self.node_id))
            else:
                self._queue_send(candidate_id,
                    protocol.encode_vote_denied(self.current_term))

    def handle_vote_granted(self, term: int, voter_id: int):
        """
        A peer granted us a vote.  Count it and become leader if we have majority.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)
                return
            # Only count votes if we are still a candidate in this term
            if self.state != "CANDIDATE" or term != self.current_term:
                return

            self.votes_received.add(voter_id)
            # votes_received already contains self.node_id (added in _start_election),
            # so no +1 needed here — own vote is already counted inside the set.
            total = len(self.votes_received)
            print(f"[RAFT {self.node_id}] Vote from Node {voter_id} | total={total}/{self.cluster_size}")

            # Majority = more than half the cluster (e.g. 2 out of 3, 3 out of 5)
            if total > self.cluster_size // 2:
                self._become_leader()

    def handle_vote_denied(self, term: int):
        """
        A peer refused to vote for us.  If it revealed a higher term, step down.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)

    def handle_replicate(self, term: int, leader_id: int, prev_log_index: int,
                         prev_log_term: int, entries: list, commit_index: int):
        """
        Leader is sending us log entries (or a heartbeat with entries=[]).

        Steps:
          1. Verify the message is from a valid leader (term check).
          2. Verify our log is consistent with the leader at prev_log_index.
          3. Append the new entries, overwriting any conflicting ones.
          4. Advance our commit_index.
          5. Send REPLICATE_ACK back to leader.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)

            # Reject stale messages from old leaders
            if term < self.current_term:
                self._queue_send(leader_id, protocol.encode_replicate_ack(
                    self.current_term, self.node_id, -1, False))
                return

            # Valid message from current leader
            self.leader_id = leader_id
            if self.state != "FOLLOWER":
                self._step_down(term)
            self._reset_timer_locked()  # reset election timer since leader is alive

            # --- Log consistency check ---
            # The leader says "the entry just before mine is at prev_log_index with term prev_log_term".
            # Our log must agree on that entry or we're out of sync.
            if prev_log_index >= 0:
                if prev_log_index >= len(self.raft_log):
                    # We don't even have that entry yet
                    self._queue_send(leader_id, protocol.encode_replicate_ack(
                        self.current_term, self.node_id, len(self.raft_log) - 1, False))
                    return
                if self.raft_log[prev_log_index]["term"] != prev_log_term:
                    # Entry exists but with a different term — conflict
                    self.raft_log = self.raft_log[:prev_log_index]  # truncate
                    self._queue_send(leader_id, protocol.encode_replicate_ack(
                        self.current_term, self.node_id, len(self.raft_log) - 1, False))
                    return

            # --- Append new entries ---
            for entry in entries:
                idx = entry["index"]
                if idx < len(self.raft_log):
                    if self.raft_log[idx]["term"] != entry["term"]:
                        # Conflict: overwrite from this index onwards
                        self.raft_log = self.raft_log[:idx]
                        self.raft_log.append(entry)
                else:
                    self.raft_log.append(entry)

            # --- Advance commit_index ---
            # Followers learn the committed index from the leader's REPLICATE message.
            # This is how RAFT propagates commit decisions to all nodes.
            if commit_index > self.commit_index:
                new_commit = min(commit_index, len(self.raft_log) - 1)
                if new_commit > self.commit_index:
                    print(f"[RAFT {self.node_id}] FOLLOWER commit_index advanced "
                          f"{self.commit_index} → {new_commit} (leader={leader_id})")
                self.commit_index = new_commit

            match_index = len(self.raft_log) - 1
            self._queue_send(leader_id, protocol.encode_replicate_ack(
                self.current_term, self.node_id, match_index, True))

    def handle_replicate_ack(self, term: int, follower_id: int,
                             match_index: int, success: bool):
        """
        A follower responded to our REPLICATE message.
        - success=True:  update our record of what that follower has, check if
                         we can now commit more entries.
        - success=False: the follower's log was behind.  Back up next_index and retry.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)
                return
            if self.state != "LEADER":
                return

            if success:
                # Update match_index (highest replicated index on this follower)
                prev = self.match_index.get(follower_id, -1)
                self.match_index[follower_id] = max(prev, match_index)
                self.next_index[follower_id]  = self.match_index[follower_id] + 1
                # Check if any new entries can be committed
                self._try_advance_commit()
            else:
                # Back up by 1 and resend
                ni = self.next_index.get(follower_id, len(self.raft_log))
                self.next_index[follower_id] = max(0, ni - 1)
                self._queue_replicate_to(follower_id)

    # =========================================================================
    # State Transition Helpers  (must be called with self._lock held)
    # =========================================================================

    def _step_down(self, new_term: int):
        """
        Revert to FOLLOWER.  Called whenever we see a term higher than ours,
        or when a valid leader sends us a message.
        """
        print(f"[RAFT {self.node_id}] Stepping down to FOLLOWER | term {self.current_term} → {new_term}")
        self.state        = "FOLLOWER"
        self.current_term = new_term
        self.voted_for    = None    # reset vote for the new term
        self._reset_timer_locked()

    def _start_election(self):
        """
        Transition to CANDIDATE and broadcast VOTE_REQUEST to all peers.
        Called by the election timer thread when the timeout fires.
        """
        self.state         = "CANDIDATE"
        self.current_term += 1      # each election uses a new, higher term
        self.voted_for     = self.node_id
        self.votes_received = {self.node_id}    # vote for ourselves
        self.leader_id     = None
        self._reset_timer_locked()  # set new timeout in case this election fails

        last_log_index = len(self.raft_log) - 1
        last_log_term  = self.raft_log[-1]["term"] if self.raft_log else 0

        msg = protocol.encode_vote_request(
            self.current_term, self.node_id, last_log_index, last_log_term)

        print(f"[RAFT {self.node_id}] Starting ELECTION | term={self.current_term}")
        for peer_id in self.peers:
            self._queue_send(peer_id, msg)

    def _become_leader(self):
        """
        Transition to LEADER.  Set up leader state and start heartbeats.
        """
        print(f"[RAFT {self.node_id}] *** BECAME LEADER | term={self.current_term} ***")
        self.state     = "LEADER"
        self.leader_id = self.node_id

        # Initialize per-follower tracking
        log_len = len(self.raft_log)
        for peer_id in self.peers:
            self.next_index[peer_id]  = log_len  # optimistic: send from end
            self.match_index[peer_id] = -1

        # Start heartbeat loop in a background thread
        threading.Thread(target=self._heartbeat_loop,
                         daemon=True, name="raft-heartbeat").start()

        # Send an immediate heartbeat to assert leadership right away
        self._queue_replicate_all()

    # =========================================================================
    # Background Loops
    # =========================================================================

    def _election_timer_loop(self):
        """
        Polls every 100 ms.  If the election deadline has passed without a
        heartbeat from the leader, this node starts a new election.
        """
        while True:
            time.sleep(0.1)
            with self._lock:
                if self.state == "LEADER":
                    # Leaders don't need an election timer; keep resetting it
                    # so it doesn't fire immediately when we step down later.
                    self._reset_timer_locked()
                    continue
                if time.time() >= self._timer_deadline:
                    self._start_election()

    def _heartbeat_loop(self):
        """
        Runs only while this node is the leader.
        Sends heartbeats / pending log entries to followers every
        HEARTBEAT_INTERVAL seconds.
        """
        while True:
            time.sleep(config.HEARTBEAT_INTERVAL)
            with self._lock:
                if self.state != "LEADER":
                    return  # stop when we're no longer leader
                self._queue_replicate_all()

    def _apply_loop(self):
        """
        Watches commit_index.  When new entries are committed, applies them
        to the LogStore (writes to disk).
        Runs forever in a background thread.
        """
        while True:
            time.sleep(0.05)
            with self._lock:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.raft_log[self.last_applied]
                    # Apply outside the tight inner section but still inside lock
                    # (LogStore uses its own internal lock so this is fine)
                    self._apply_entry(entry)

    def _send_worker(self):
        """
        Background thread that drains the outgoing message queue and sends
        each (peer_id, message) pair to the corresponding broker.
        By doing this outside self._lock, we avoid holding the lock during
        potentially slow network operations.
        """
        while True:
            peer_id, message = self._send_queue.get()
            try:
                self._send_raw(peer_id, message)
            except Exception as e:
                # Peer might be down; that's okay — RAFT handles node failures
                pass

    # =========================================================================
    # Private Helpers  (called with self._lock held unless noted)
    # =========================================================================

    def _reset_timer_locked(self):
        """
        Set a new random election deadline.
        Randomness prevents all followers from timing out simultaneously.
        MUST be called with self._lock held.
        """
        timeout = random.uniform(
            config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self._timer_deadline = time.time() + timeout

    def _queue_send(self, peer_id: int, message: str):
        """
        Enqueue a message to be sent to peer_id.
        SAFE to call with self._lock held (does not block).
        """
        self._send_queue.put((peer_id, message))

    def _queue_replicate_all(self):
        """Send REPLICATE to every follower. Called with lock held."""
        for peer_id in self.peers:
            self._queue_replicate_to(peer_id)

    def _queue_replicate_to(self, peer_id: int):
        """
        Build a REPLICATE message for a specific follower and enqueue it.
        Called with lock held.
        """
        next_idx = self.next_index.get(peer_id, len(self.raft_log))

        # Entries the follower is missing
        entries_to_send = self.raft_log[next_idx:]

        # The entry just before what we're sending (for consistency check)
        prev_idx  = next_idx - 1
        prev_term = (self.raft_log[prev_idx]["term"]
                     if 0 <= prev_idx < len(self.raft_log) else 0)

        msg = protocol.encode_replicate(
            self.current_term, self.node_id,
            prev_idx, prev_term,
            entries_to_send,
            self.commit_index,
        )
        self._queue_send(peer_id, msg)

    def _try_advance_commit(self):
        """
        After a successful REPLICATE_ACK, check if any new entries can be
        committed (i.e., stored on a majority of nodes).

        RAFT safety rule: only commit entries from the CURRENT term.
        (This prevents re-committing entries from an old leader's term.)
        Called with lock held.
        """
        if not self.raft_log:
            return

        # Walk backwards from the last log entry to commit_index+1
        for n in range(len(self.raft_log) - 1, self.commit_index, -1):
            if self.raft_log[n]["term"] != self.current_term:
                continue    # RAFT safety: skip entries from older terms

            # Count nodes that have this entry
            count = 1   # the leader itself
            for peer_id in self.peers:
                if self.match_index.get(peer_id, -1) >= n:
                    count += 1

            if count > self.cluster_size // 2:
                print(f"[RAFT {self.node_id}] COMMITTED up to index {n}")
                self.commit_index = n
                break

    def _log_is_ok(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        """
        RAFT safety check: is the candidate's log at least as up-to-date as ours?
        Called during vote request handling with lock held.

        A log A is more up-to-date than log B if:
          - A's last term > B's last term, OR
          - Same last term but A is longer (higher index)
        """
        our_last_term  = self.raft_log[-1]["term"] if self.raft_log else 0
        our_last_index = len(self.raft_log) - 1

        if candidate_last_term != our_last_term:
            return candidate_last_term > our_last_term
        return candidate_last_index >= our_last_index

    def _apply_entry(self, entry: dict):
        """
        Write a committed RAFT entry to the on-disk LogStore.
        Called with self._lock held (LogStore has its own internal lock).
        """
        entry_type = entry.get("type")
        topic      = entry.get("topic", "")
        data       = entry.get("data", "")

        if entry_type == "CREATE_TOPIC":
            created = self.log_store.create_topic(topic)
            if created:
                print(f"[RAFT {self.node_id}] Applied CREATE_TOPIC → '{topic}'")

        elif entry_type == "METRIC":
            if self.log_store.topic_exists(topic):
                # Pass the timestamp that was recorded by the LEADER at proposal
                # time — this makes all broker log files byte-identical.
                ts = entry.get("ts")
                self.log_store.append(topic, data, ts=ts)
            else:
                print(f"[RAFT {self.node_id}] WARNING: METRIC for unknown topic '{topic}' — ignoring")
