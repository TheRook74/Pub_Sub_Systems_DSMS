# =============================================================================
# start_cluster.py
#
# Convenience script to start all 3 broker nodes at once in separate
# terminal subprocesses.  Useful during development and testing.
#
# Usage:
#   python start_cluster.py          → starts all 3 brokers
#   python start_cluster.py --stop   → kill any running broker processes
#
# What it does:
#   - Launches  "python broker/broker.py --id 0"  in a new terminal window
#   - Launches  "python broker/broker.py --id 1"  in a new terminal window
#   - Launches  "python broker/broker.py --id 2"  in a new terminal window
#
# Each broker runs independently.  You can also start them manually in
# three separate terminal windows — this script is just a shortcut.
#
# NOTE: On Windows this opens new CMD windows.  On Linux/macOS it opens
# new terminal tabs/windows depending on the terminal emulator available.
# =============================================================================

import subprocess
import sys
import os
import time
import platform

# The directory that contains this script (= dsms/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the broker script
BROKER_SCRIPT = os.path.join(BASE_DIR, "broker", "broker.py")

# Number of broker nodes (must match len(config.BROKERS))
NUM_BROKERS = 3


def start_brokers():
    """Launch each broker node in a separate process."""
    processes = []
    print(f"[start_cluster] Starting {NUM_BROKERS} broker nodes...")

    for node_id in range(NUM_BROKERS):
        cmd = [sys.executable, BROKER_SCRIPT, "--id", str(node_id)]

        if platform.system() == "Windows":
            # On Windows: open a new CMD window for each broker
            proc = subprocess.Popen(
                ["cmd", "/c", "start", "cmd", "/k"] + cmd,
                cwd=BASE_DIR,
            )
        else:
            # On Linux/macOS: run in background (output goes to a log file)
            log_path = os.path.join(BASE_DIR, f"broker_node{node_id}.log")
            with open(log_path, "w") as log_f:
                proc = subprocess.Popen(
                    cmd,
                    cwd=BASE_DIR,
                    stdout=log_f,
                    stderr=log_f,
                )
            print(f"  Node {node_id} → PID {proc.pid} | log: {log_path}")

        processes.append(proc)

    print("\n[start_cluster] All brokers started.")
    print("Wait ~3 seconds for the cluster to elect a leader, then run:")
    print("  python publisher/dsms_pub.py")
    print("  python subscriber/dsms_sub.py")
    return processes


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Start or stop the DSMS broker cluster.")
    parser.add_argument("--stop", action="store_true",
                        help="Kill any running broker processes (Linux/macOS only)")
    args = parser.parse_args()

    if args.stop:
        if platform.system() != "Windows":
            os.system(f"pkill -f 'broker.py'")
            print("[start_cluster] Sent kill signal to broker processes.")
        else:
            print("[start_cluster] On Windows, close the broker CMD windows manually.")
        return

    procs = start_brokers()

    if platform.system() != "Windows":
        # On Linux/macOS, keep the script running and wait for Ctrl+C
        print("\n[start_cluster] Cluster is running.  Press Ctrl+C to stop all brokers.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[start_cluster] Stopping all brokers...")
            for p in procs:
                p.terminate()
            print("[start_cluster] Done.")


if __name__ == "__main__":
    main()
