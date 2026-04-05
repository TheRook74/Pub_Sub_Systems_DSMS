# =============================================================================
# publisher/metrics.py
#
# Collects CPU and memory usage for running OS processes using the psutil
# library.
#
# psutil (Process and System Utilities) lets Python read real-time system
# info such as CPU %, memory %, running process names, etc., on Windows,
# Linux, and macOS.
#
# How it's used:
#   The publisher calls get_metrics_for_app("chrome") and gets back a string
#   like "cpu=15.3 memory=3.4" which it then attaches to a METRIC message
#   and sends to the broker.
# =============================================================================

import psutil


def get_metrics_for_app(app_name: str) -> str:
    """
    Find the OS process whose name contains app_name (case-insensitive)
    and return its current CPU% and memory% as a formatted string.

    Returns a string like:  "cpu=15.3 memory=3.4"
    Returns None if no matching process is found.

    Note on CPU measurement:
        psutil.cpu_percent() with interval=None returns the CPU% since the
        LAST call.  The first call always returns 0.0 for a process, which
        is normal.  On the second call (after some time has passed) it gives
        a meaningful value.  That's why we call it with interval=0.1 here
        to get an immediate (slightly delayed) reading.
    """
    for proc in psutil.process_iter(["name", "status"]):
        try:
            proc_name = proc.info["name"] or ""
            # Case-insensitive partial match: "chrome" matches "chrome.exe"
            if app_name.lower() in proc_name.lower():
                # cpu_percent(interval=0.1): block for 0.1s to get a real reading
                cpu = proc.cpu_percent(interval=0.1)
                # memory_percent(): percentage of total RAM used by this process
                mem = proc.memory_percent()
                return f"cpu={cpu:.1f} memory={mem:.2f}"
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Process disappeared or we can't read it — skip it
            continue

    # No running process with that name found
    return None


def get_system_metrics() -> str:
    """
    Return overall system CPU and memory usage as a fallback.
    Used when the specific app process is not found but we still want
    to send some data (e.g. for testing the broker pipeline).
    """
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory().percent
    return f"cpu={cpu:.1f} memory={mem:.1f}"


def list_running_processes() -> list:
    """
    Helper / debug function.
    Returns a list of (pid, name) tuples for all currently running processes.
    Useful when setting up apps.json to find the exact process name.
    """
    procs = []
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            procs.append((proc.info["pid"], proc.info["name"]))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return sorted(procs, key=lambda x: x[1].lower())
