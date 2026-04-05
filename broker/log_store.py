# =============================================================================
# broker/log_store.py
#
# Disk-based, append-only log storage for topic data.
#
# Design principles (inspired by Apache Kafka's log-based architecture):
#   - Each topic has its own dedicated log FILE on disk.
#   - New entries are always APPENDED to the end (never overwritten).
#   - Subscribers read using a BYTE OFFSET: they remember where they stopped
#     last time and ask "give me everything after byte N".  This means they
#     never re-read old data and never miss new data.
#   - Log files survive broker crashes (data is on disk, not in RAM).
#
# Topic naming → file name mapping:
#   "app1.server1"  →  <base_dir>/app1_server1.log
#   Dots are replaced with underscores because dot has meaning in file paths.
# =============================================================================

import os
import json
import time
import threading


class LogStore:
    """
    Manages append-only log files for every topic on this broker node.
    Thread-safe: all file operations are protected by a lock.
    """

    def __init__(self, base_dir: str = "logs"):
        """
        base_dir: directory where this broker stores its log files.
        Each broker node uses a different base_dir so they don't share files
        (e.g. 'broker_data/node0', 'broker_data/node1', ...).
        """
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)  # create dir if it doesn't exist

        # A threading lock prevents two threads from writing the same file at
        # the same time, which would corrupt the log.
        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Private helper
    # -------------------------------------------------------------------------

    def _get_path(self, topic: str) -> str:
        """
        Convert a topic name to an absolute file path.
        Example: 'app1.server1' → '/path/to/logs/app1_server1.log'
        """
        safe_name = topic.replace(".", "_")
        return os.path.join(self.base_dir, f"{safe_name}.log")

    # -------------------------------------------------------------------------
    # Topic management
    # -------------------------------------------------------------------------

    def create_topic(self, topic: str) -> bool:
        """
        Create an empty log file for a topic if it does not already exist.
        Returns True if the topic was newly created, False if it already existed.
        """
        path = self._get_path(topic)
        with self._lock:
            if not os.path.exists(path):
                # open(..., 'a') creates the file and leaves it empty
                open(path, "a").close()
                print(f"[LogStore] Created topic file: {path}")
                return True
        return False

    def topic_exists(self, topic: str) -> bool:
        """Return True if the topic's log file exists on this broker."""
        return os.path.exists(self._get_path(topic))

    def all_topics(self) -> list:
        """
        Return a list of all topic names stored on this broker.
        Used when a new broker joins and needs to know what topics exist.
        """
        topics = []
        for fname in os.listdir(self.base_dir):
            if fname.endswith(".log"):
                # Convert filename back to topic: 'app1_server1.log' → 'app1.server1'
                topic = fname[:-4].replace("_", ".")
                topics.append(topic)
        return topics

    # -------------------------------------------------------------------------
    # Write
    # -------------------------------------------------------------------------

    def append(self, topic: str, data: str, ts: str = None):
        """
        Append a new metric entry to the topic's log file.

        Each entry is a JSON line with:
            "ts"   : timestamp — provided by the RAFT log entry so that ALL
                     broker nodes write the EXACT same timestamp regardless of
                     when each node's apply-thread runs.  If ts is not given,
                     we fall back to the current wall-clock time.
            "data" : the raw metric string, e.g. "cpu=70.5 memory=45.2"

        Example line written to disk:
            {"ts": "2026-04-05 10:30:01", "data": "cpu=70.5 memory=45.2"}

        WHY pass ts from outside?
            The leader proposes an entry and embeds a timestamp at that moment.
            Followers apply the same entry seconds later.  If each node called
            time.strftime() independently, files would have different timestamps
            → byte-for-byte content would differ even though the logical data
            is identical.  By using the leader's timestamp everywhere, all three
            log files end up byte-identical — which is the correct behaviour for
            a replicated log.
        """
        path = self._get_path(topic)
        if ts is None:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

        entry = json.dumps({
            "ts":   ts,
            "data": data,
        }) + "\n"   # the newline is important — one JSON object per line

        with self._lock:
            with open(path, "a") as f:
                f.write(entry)

    # -------------------------------------------------------------------------
    # Read
    # -------------------------------------------------------------------------

    def read(self, topic: str, byte_offset: int = 0):
        """
        Read log entries starting from byte_offset.

        How byte-offset reading works:
            - The first time a subscriber reads, it sends offset=0 (start of file).
            - The broker reads from byte 0 to end of file and returns:
                content_bytes  = those bytes
                new_offset     = byte position after the last byte returned
            - Next time, subscriber sends new_offset.  The broker seeks directly
              to that position and returns only the NEW bytes written since then.
            - This is very efficient: no scanning the whole file every time.

        Returns:
            (content_bytes: bytes, new_offset: int)
            content_bytes is empty (b"") if there is nothing new to read.
        """
        path = self._get_path(topic)

        if not os.path.exists(path):
            # Topic doesn't exist on this broker — return empty
            return b"", 0

        with self._lock:
            with open(path, "rb") as f:       # open in binary mode for bytes
                f.seek(byte_offset)           # jump to where we left off
                content = f.read()            # read everything from there to end
            new_offset = byte_offset + len(content)

        return content, new_offset
