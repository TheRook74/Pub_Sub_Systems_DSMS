# =============================================================================
# common/config.py
#
# Central configuration file for the entire DSMS cluster.
# Every component (broker, publisher, subscriber) imports constants from here.
# To change cluster size or ports, edit ONLY this file.
# =============================================================================

# ---------------------------------------------------------------------------
# Broker Cluster Definition
# ---------------------------------------------------------------------------
# Each broker is identified by a unique integer 'id', a 'host' address, and a
# TCP 'port' number.  When running everything on one machine (localhost), we
# give each broker a different port so they don't clash.
BROKERS = [
    {"id": 0, "host": "127.0.0.1", "port": 5000},
    {"id": 1, "host": "127.0.0.1", "port": 5001},
    {"id": 2, "host": "127.0.0.1", "port": 5002},
]

# ---------------------------------------------------------------------------
# RAFT Timing Parameters
# ---------------------------------------------------------------------------
# ELECTION_TIMEOUT: how long a follower waits (in seconds) before assuming
# the leader is dead and starting a new election.
# We pick a RANDOM value in [MIN, MAX] so all nodes don't time out at exactly
# the same moment (which would cause a "split vote" where nobody wins).
ELECTION_TIMEOUT_MIN = 2.0   # seconds
ELECTION_TIMEOUT_MAX = 4.0   # seconds

# HEARTBEAT_INTERVAL: how often (in seconds) the current leader broadcasts
# a heartbeat/replication message to followers.
# Rule: heartbeat << election_timeout  so followers never falsely time out.
HEARTBEAT_INTERVAL = 0.75    # seconds

# ---------------------------------------------------------------------------
# Publisher Parameters
# ---------------------------------------------------------------------------
# How often (in seconds) DSMS_PUB collects and sends fresh CPU/memory metrics.
PUBLISH_INTERVAL = 5         # seconds

# Maximum number of times the publisher retries a failed broker contact
# before giving up on creating a topic.
MAX_RETRIES = 10

# How long (seconds) to wait between retries when the cluster has no leader yet.
RETRY_DELAY = 1.0            # seconds

# ---------------------------------------------------------------------------
# Subscriber Parameters
# ---------------------------------------------------------------------------
# How often (in seconds) DSMS_SUB polls the broker for new log entries.
POLL_INTERVAL = 3            # seconds

# Port on which the DSMS_SUB HTTP REST API is exposed.
# Clients (dashboards, alert systems) query this port.
SUBSCRIBER_HTTP_PORT = 8080
