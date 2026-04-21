# DSMS — Run Commands Cheat Sheet

Every command is given **twice**: once for **Windows PowerShell** and once for
**Linux / macOS (bash / zsh)**. Pick the block that matches your OS.

Before anything else, open a terminal in the `dsms/` folder:

```powershell
# Windows PowerShell
cd "C:\path\to\dsms"
```

```bash
# Linux / macOS
cd /path/to/dsms
```

---

## 1. One-time setup

### 1a. Install Python dependencies

```powershell
# Windows PowerShell
python -m pip install -r requirements.txt
```

```bash
# Linux / macOS
python3 -m pip install -r requirements.txt
```

### 1b. Configure this machine in `common/config.py`

Edit the `BROKERS` list so each entry has the real LAN IP of the machine that
will host that broker id. Every machine must see the **same** `config.py`.

```python
BROKERS = [
    {"id": 0, "host": "10.0.0.10", "port": 5000},
    {"id": 1, "host": "10.0.0.11", "port": 5001},
    {"id": 2, "host": "10.0.0.12", "port": 5002},
    # Add more nodes here as you scale the cluster.
]
```

To see this machine's LAN IP:

```powershell
# Windows PowerShell
ipconfig | Select-String "IPv4"
```

```bash
# Linux / macOS
# Prefer this if it's installed, otherwise fall back to ifconfig.
hostname -I 2>/dev/null || ifconfig | grep "inet "
```

---

## 2. Firewall rules (open once, per machine)

Each broker must accept incoming TCP on its own broker port. The subscriber
also exposes an HTTP API that other tools may query on port `8080`.

### Windows (PowerShell as Administrator)

```powershell
# Replace <BROKER_PORT> with this machine's port (5000, 5001, 5002, ...).
# Run PowerShell "as Administrator" or the rule will silently fail.

# Inbound rule for this machine's broker port.
New-NetFirewallRule -DisplayName "DSMS Broker <BROKER_PORT>" `
    -Direction Inbound -Protocol TCP -LocalPort <BROKER_PORT> -Action Allow

# Inbound rule for the subscriber HTTP API (same on every machine).
New-NetFirewallRule -DisplayName "DSMS Subscriber 8080" `
    -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
```

To remove the rules afterwards:

```powershell
Remove-NetFirewallRule -DisplayName "DSMS Broker <BROKER_PORT>"
Remove-NetFirewallRule -DisplayName "DSMS Subscriber 8080"
```

### Linux (ufw — Ubuntu / Debian)

```bash
# Replace <BROKER_PORT> with this machine's broker port.
sudo ufw allow <BROKER_PORT>/tcp comment "DSMS broker"
sudo ufw allow 8080/tcp          comment "DSMS subscriber HTTP API"
sudo ufw reload
```

To remove later:

```bash
sudo ufw delete allow <BROKER_PORT>/tcp
sudo ufw delete allow 8080/tcp
```

### Linux (firewalld — Fedora / RHEL / CentOS)

```bash
sudo firewall-cmd --permanent --add-port=<BROKER_PORT>/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload
```

### macOS

The default macOS firewall allows outgoing connections and only blocks
incoming connections for **signed** applications. Python is usually not
signed, so the first time you start a broker you will get a popup — click
"Allow". No CLI configuration is needed in the common case.

If the application firewall is fully enabled and you want to pre-authorize
Python:

```bash
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /usr/bin/python3
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp /usr/bin/python3
```

---

## 3. Reset the node (wipe all state)

Run this **before every fresh demo** on every machine in the cluster.

```powershell
# Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\reset.ps1
```

```bash
# Linux / macOS
chmod +x reset.sh   # first time only
./reset.sh
```

What it wipes: leftover python processes, `broker_data/` (RAFT meta + RAFT
log + topic log files), and `__pycache__/` folders.

---

## 4. Start a broker on this machine

One broker per machine. Pass this machine's node id (must be listed in
`common/config.py`).

```powershell
# Windows PowerShell  — run in its OWN terminal, leave it open
python broker/broker.py --id <NODE_ID>
```

```bash
# Linux / macOS  — run in its OWN terminal, leave it open
python3 broker/broker.py --id <NODE_ID>
```

To cleanly stop it, press `Ctrl+C` in that terminal.

---

## 5. Start the publisher on this machine

Each machine may (optionally) run a publisher that monitors its own apps.
Use one of the `apps*.json` files and point the publisher at it.

```powershell
# Windows PowerShell
python publisher/dsms_pub.py --apps publisher/apps.json
```

```bash
# Linux / macOS
python3 publisher/dsms_pub.py --apps publisher/apps.json
```

If you want multiple publishers per machine (different app lists), just pass
different config files in separate terminals:

```powershell
python publisher/dsms_pub.py --apps publisher/apps_pub2.json
python publisher/dsms_pub.py --apps publisher/apps_pub3.json
```

```bash
python3 publisher/dsms_pub.py --apps publisher/apps_pub2.json
python3 publisher/dsms_pub.py --apps publisher/apps_pub3.json
```

---

## 6. Start the subscriber on this machine

The subscriber exposes an HTTP REST API on port `8080` (configurable via
`SUBSCRIBER_HTTP_PORT` in `common/config.py`).

```powershell
# Windows PowerShell
python subscriber/dsms_sub.py
```

```bash
# Linux / macOS
python3 subscriber/dsms_sub.py
```

---

## 7. Talk to the subscriber (HTTP client commands)

Replace `<host>` with this machine's LAN IP if you query from another box.
If you are on the same machine, use `localhost`.

### Subscribe to a topic

```powershell
# Windows PowerShell — use Invoke-WebRequest or curl.exe
Invoke-WebRequest -Method POST -Uri "http://<host>:8080/subscribe?topic=brave.server0"
# Or:
curl.exe -X POST "http://<host>:8080/subscribe?topic=brave.server0"
```

```bash
# Linux / macOS
curl -X POST "http://<host>:8080/subscribe?topic=brave.server0"
```

### Fetch log entries for a topic (exact match)

```powershell
# Windows PowerShell
Invoke-WebRequest -Uri "http://<host>:8080/logs?topic=brave.server0"
```

```bash
# Linux / macOS
curl "http://<host>:8080/logs?topic=brave.server0"
```

### Aggregate logs by app prefix (e.g. all servers running `brave`)

```powershell
Invoke-WebRequest -Uri "http://<host>:8080/logs?topic=brave"
```

```bash
curl "http://<host>:8080/logs?topic=brave"
```

### Aggregate logs by server suffix (e.g. all apps on `server0`)

```powershell
Invoke-WebRequest -Uri "http://<host>:8080/logs?topic=server0"
```

```bash
curl "http://<host>:8080/logs?topic=server0"
```

### Unsubscribe from a topic

```powershell
Invoke-WebRequest -Method DELETE -Uri "http://<host>:8080/subscribe?topic=brave.server0"
# Or:
curl.exe -X DELETE "http://<host>:8080/subscribe?topic=brave.server0"
```

```bash
curl -X DELETE "http://<host>:8080/subscribe?topic=brave.server0"
```

### Show all active subscriptions + entry counts

```powershell
Invoke-WebRequest -Uri "http://<host>:8080/status"
```

```bash
curl "http://<host>:8080/status"
```

---

## 8. Diagnostic commands

### Is the broker listening on its port?

```powershell
# Windows PowerShell — replace <BROKER_PORT>
netstat -ano | findstr :<BROKER_PORT>
```

```bash
# Linux / macOS
ss -tlnp | grep :<BROKER_PORT>         # Linux
lsof -i :<BROKER_PORT>                 # macOS (lsof also works on Linux)
```

### Can this machine reach another broker over the network?

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <REMOTE_IP> -Port <BROKER_PORT>
```

```bash
# Linux / macOS
# -zv = zero-I/O mode + verbose, nc exits 0 if the port is open.
nc -zv <REMOTE_IP> <BROKER_PORT>
```

### Verify the replicated log files are byte-identical across nodes

After running for a minute, the same topic file should be the same size on
every broker. Run this on each machine and compare:

```powershell
# Windows PowerShell
Get-ChildItem .\broker_data\node<NODE_ID>\*.log | Select-Object Name, Length
```

```bash
# Linux / macOS
ls -l ./broker_data/node<NODE_ID>/*.log
```

---

## 9. Kill a specific broker (for failover tests)

Simply press `Ctrl+C` in the broker's terminal. If you need to kill it from
another shell:

```powershell
# Windows PowerShell — find PID then stop.
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object { $_.CommandLine -match "broker/broker.py" } |
    Select-Object ProcessId, CommandLine
Stop-Process -Id <PID> -Force
```

```bash
# Linux / macOS
pgrep -a -f "broker/broker.py"     # list PIDs + command lines
kill <PID>                         # graceful shutdown
kill -9 <PID>                      # only if graceful kill does not work
```

---

## 10. Typical demo run (quick reference)

Do this on **every machine** that hosts a broker:

1. `cd` into `dsms/`
2. Reset: `.\reset.ps1` or `./reset.sh`
3. Start broker in terminal 1: `python broker/broker.py --id <NODE_ID>`
4. Start publisher in terminal 2: `python publisher/dsms_pub.py --apps publisher/apps.json`
5. Start subscriber in terminal 3: `python subscriber/dsms_sub.py`
6. Use the HTTP client commands in section 7 to subscribe and query.

To demonstrate fault tolerance: pick any broker terminal, hit `Ctrl+C`. The
other brokers elect a new leader; publishers and subscribers keep working as
long as a majority of brokers remain alive.
