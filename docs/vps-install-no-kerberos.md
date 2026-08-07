# Installing cdp-mcp on a VPS — non-Kerberized cluster

This is the **simple** deployment guide: a long-running `cdp-mcp` daemon on a
Linux VPS for a CDP cluster that is **not** Kerberized — CM uses Basic auth, the
downstream service endpoints (NameNode JMX, YARN RM, Spark HS, Oozie) are reached
directly, and there is no SPNEGO, no `kinit`, and no keytab.

> If your cluster **is** Kerberized (downstream endpoints challenge with
> `401 Negotiate`), or sits behind a jump host with internal-only hostnames, use
> the [VPS install with autossh/SPNEGO](vps-install.md) instead — that guide adds
> the autossh SOCKS tunnel and keytab/Kerberos layers this one deliberately omits.

This guide covers the one persistent piece you need here:

- **`cdp-mcp`** — the MCP server (systemd service).

---

## 0. Assumptions

- VPS: a modern RHEL/Rocky/Alma or Debian/Ubuntu box with root/sudo.
- The VPS can reach the Cloudera Manager host and the cluster's service
  endpoints **directly** over HTTP/HTTPS (no jump host, no SOCKS proxy needed).
- The cluster is **not** Kerberized — `kerberos: false` (the default) on the CM
  instance. If a downstream endpoint returns `401 Negotiate`, you're on the wrong
  guide; switch to [vps-install.md](vps-install.md).

If the cluster is internal-only (the VPS can't resolve its hostnames directly)
but still not Kerberized, see the **optional** [§6](#6-optional-autossh-tunnel-for-an-internal-only-cluster)
at the end — you may need the SOCKS tunnel for *network* reasons even without
Kerberos.

---

## 1. System packages

### RHEL / Rocky / Alma

```bash
sudo dnf install -y git python3.12 gcc make
```

### Debian / Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y git python3.12 python3.12-venv gcc make
```

No `krb5-*` and no `autossh` are needed for the non-Kerberized path.

### Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"   # per the installer's output
```

---

## 2. Install cdp-mcp

```bash
cd /opt
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server

uv venv --python 3.12
# Install from uv.lock (base + dev). Use `uv sync`, NOT a lockless
# `uv pip install -e '.[dev]'`: the latter resolves fresh and can pull an
# incompatible major `mcp` (2.0 removed the `mcp.server.fastmcp` import
# server.py uses). uv.lock pins the known-good 1.x.
uv sync --extra dev

.venv/bin/cdp-mcp --help    # must print help
```

You do **not** need the `[kerberos]` extra — it pulls in `httpx-gssapi`/`gssapi`
(MIT krb5 native build), which is only required when a CM instance has
`kerberos: true`. Skipping it keeps the install lighter and krb5-free.

---

## 3. Registry config

```bash
cp cm_instances.yaml.example cm_instances.yaml
```

Edit `cm_instances.yaml`. For a non-Kerberized cluster, leave `kerberos` off
(the default) and omit the `kerberos_keytab`/`kerberos_principal` fields:

```yaml
instances:
  - host: cm.example.com
    port: 7183
    username: admin
    password: "${CM_PASSWORD}"      # pulled from the env file below
    environment_name: prod
    use_tls: false                   # set true + verify_ssl for HTTPS
    verify_ssl: false
    api_version: v51                  # adjust to your CM version (v40–v54)
    timeout_seconds: 30
    downstream_timeout_seconds: 30
    # kerberos: false                 # (default) — downstream clients use no auth
    # Outbound proxy (if the cluster is internal-only): NOT a yaml field — set
    # the ALL_PROXY env var on the systemd unit, e.g.
    # ALL_PROXY=socks5h://127.0.0.1:1080 (httpx trust_env honors it). See §6.
    active: true
```

Put the secret in `/etc/cdp-mcp/cdp-mcp.env` (gitignored, `chmod 600`):

```bash
sudo mkdir -p /etc/cdp-mcp
sudo install -m 600 /dev/null /etc/cdp-mcp/cdp-mcp.env
sudo tee -a /etc/cdp-mcp/cdp-mcp.env >/dev/null <<'EOF'
CM_PASSWORD=changeme
EOF
```

> No `KRB5CCNAME` (Kerberos ccache) is ever needed here. `ALL_PROXY` is only
> needed if the cluster is internal-only — see [§6](#6-optional-autossh-tunnel-for-an-internal-only-cluster).
> The Kerberized/jump-host guide is [vps-install.md](vps-install.md).

---

## 4. cdp-mcp systemd service

`/etc/systemd/system/cdp-mcp.service`

```ini
[Unit]
Description=cdp-mcp MCP server
After=network-online.target
Wants=network-online.target
# Requires=cdp-mcp-autossh.service   # uncomment with ALL_PROXY below (§6)

[Service]
Type=simple
User=cdp
WorkingDirectory=/opt/cdp-mcp-server

# Run as a long-lived HTTP daemon (NOT stdio). A stdio server under systemd
# reads EOF on /dev/null stdin and exits cleanly right after startup — useless
# as a service. streamable-http keeps a uvicorn listener up; clients connect to
# http://<host>:8000/mcp. Bind to loopback and reach it over an SSH tunnel (§5)
# — the HTTP transport has NO built-in auth, so never set MCP_HOST=0.0.0.0 on a
# public VPS without a reverse proxy / firewall in front.
Environment=MCP_TRANSPORT=streamable-http
Environment=MCP_HOST=127.0.0.1
Environment=MCP_PORT=8000

# Registry
Environment=REGISTRY_BACKEND=file
Environment=REGISTRY_FILE_PATH=/opt/cdp-mcp-server/cm_instances.yaml

# Outbound SOCKS proxy — UNCOMMENT if the cluster is internal-only (its
# hostnames don't resolve from the VPS). Set up the autossh tunnel in §6 first,
# then uncomment the Requires= line in [Unit] and the ALL_PROXY line here.
# ALL_PROXY covers the CM client + the four downstream clients (httpx trust_env).
# Always socks5h:// (remote DNS), never socks5://.
#Environment=ALL_PROXY=socks5h://127.0.0.1:1080

# Pull CM_PASSWORD etc. from the secret file
EnvironmentFile=/etc/cdp-mcp/cdp-mcp.env

ExecStart=/opt/cdp-mcp-server/.venv/bin/cdp-mcp
Restart=on-failure
RestartSec=10

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/tmp /var/log/cdp-mcp

[Install]
WantedBy=multi-user.target
```

Create the service user and start it:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin cdp
sudo chown -R cdp:cdp /opt/cdp-mcp-server
sudo chown cdp:cdp /etc/cdp-mcp/cdp-mcp.env

sudo systemctl daemon-reload
sudo systemctl enable --now cdp-mcp.service
sudo journalctl -u cdp-mcp -f
# expect: cdp_mcp.transport transport=streamable-http ... cdp_mcpcp.ready
# (no spnego.* lines — Kerberos is off)
ss -ltnp | grep ':8000'    # listener up on loopback
```

You should see the registry load (`file_registry.loaded count=1`), then
`cdp_mcp.transport transport=streamable-http` and `cdp_mcp.ready` — the server
now stays up (no `spnego.*` lines; Kerberos is off).

> `MCP_TRANSPORT=streamable-http` is what makes the unit a real daemon. Without
> it cdp-mcp defaults to stdio and exits cleanly right after startup (stdin is
> `/dev/null` under systemd → EOF → exit 0). This unit has no `KRB5CCNAME` (no
> Kerberos ccache) and no `Requires=cdp-mcp-autossh.service` (no tunnel) — those
> belong only to the [Kerberized guide](vps-install.md).

---

## 5. Point your MCP client at the VPS

The [§4](#4-cdp-mcp-systemd-service) unit runs cdp-mcp as an HTTP server on
`127.0.0.1:8000` (path `/mcp`). It's bound to loopback with no auth, so reach it
over an SSH tunnel from the laptop.

### A. Laptop client via an SSH tunnel (recommended)

```bash
ssh -L 8000:127.0.0.1:8000 -N cdp@vps.example.com
```

```json
{
  "mcpServers": {
    "cdp": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

### B. Client on the VPS itself

```json
{
  "mcpServers": {
    "cdp": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

### Alternative: stdio over SSH (no daemon)

Skip [§4](#4-cdp-mcp-systemd-service) and let the client spawn cdp-mcp per
session over stdio:

```json
{
  "mcpServers": {
    "cdp": {
      "command": "ssh",
      "args": ["cdp@vps.example.com", "/opt/cdp-mcp-server/.venv/bin/cdp-mcp"]
    }
  }
}
```

This is per-session; the §4 HTTP daemon is one always-on process many clients share.

---

## 6. (Optional) autossh tunnel for an internal-only cluster

If the cluster is **not** Kerberized but its hostnames are internal-only (the
VPS can't resolve them), you still need a SOCKS5h proxy for *network
reachability* — just without the Kerberos layer. This is the autossh piece from
the [Kerberized guide](vps-install.md#4-autossh-socks-tunnel-network-reachability),
reproduced here for the no-SPNEGO case.

### 6.1 SSH key for the jump host

```bash
sudo -u cdp ssh-keygen -t ed25519 -N "" -f /var/lib/cdp/.ssh/jumphost_ed25519
sudo -u cdp ssh-copy-id -i /var/lib/cdp/.ssh/jumphost_ed25519.pub jumphost-user@jumphost.example.com
sudo -u cdp ssh-keyscan -H jumphost.example.com >> /var/lib/cdp/.ssh/known_hosts
```

### 6.2 autossh systemd service

`/etc/systemd/system/cdp-mcp-autossh.service`

```ini
[Unit]
Description=autossh SOCKS5 tunnel to CDP jump host for cdp-mcp
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=cdp
Environment=AUTOSSH_GATETIME=0
ExecStart=/usr/bin/autossh -M 0 -N -D 127.0.0.1:1080 \
  -o "ExitOnForwardFailure=yes" \
  -o "ServerAliveInterval=30" \
  -o "ServerAliveCountMax=3" \
  -o "StrictHostKeyChecking=yes" \
  -o "IdentityFile=/var/lib/cdp/.ssh/jumphost_ed25519" \
  jumphost-user@jumphost.example.com
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

(`autossh` needs installing: `sudo dnf install -y autossh` / `sudo apt-get install -y autossh`.)

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cdp-mcp-autossh.service
ss -ltnp | grep 1080     # confirm the SOCKS listener
```

### 6.3 Point cdp-mcp at the proxy

The [§4](#4-cdp-mcp-systemd-service) unit already carries the SOCKS lines
commented out. Uncomment them:

- in `[Unit]`: `Requires=cdp-mcp-autossh.service`
- in `[Service]`: `Environment=ALL_PROXY=socks5h://127.0.0.1:1080`

Always `socks5h://` (remote DNS), never `socks5://` — internal hostnames don't
resolve on the VPS. This `ALL_PROXY` covers both the CM client and the four
downstream clients (httpx `trust_env` honors it; no code/config field needed).
Then `sudo systemctl daemon-reload && sudo systemctl restart cdp-mcp`.

Verify reachability (no auth challenge expected on a non-Kerberized endpoint):

```bash
sudo -u cdp ALL_PROXY=socks5h://127.0.0.1:1080 \
  curl -k -s -o /dev/null -w "%{http_code}\n" \
  http://nn-1.cluster.internal:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
# expect 200 (not 401) — a 401 + Negotiate here means the cluster IS Kerberized
# and you should switch to docs/vps-install.md.
```

---

## 7. Verification

```bash
# 1. HTTP daemon up + listening on loopback?
sudo systemctl is-active cdp-mcp
ss -ltnp | grep ':8000'
sudo journalctl -u cdp-mcp --since "5 min ago" | grep -E "transport|registry|ready"
#    expect: cdp_mcp.transport transport=streamable-http  ...  cdp_mcp.ready

# 2. Local reachability of the MCP endpoint (from the VPS):
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/mcp
#    expect an HTTP response (any code proves the listener is up).

# 3. Quick tool smoke test over HTTP (streamable-http) — list clusters:
curl -s -X POST http://127.0.0.1:8000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_clusters","arguments":{}}}'
```

On an unfamiliar cluster, the first MCP tool to call is `get_cluster_security_info`
(confirm TLS/Kerberos status — should show Kerberos off), then `list_roles` to see
the service URLs auto-discovery will use.

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `cdp-mcp.service` exits 0 right after `cdp_mcp.ready` ("Deactivated successfully") | Missing `MCP_TRANSPORT=streamable-http` on the unit → defaults to stdio → reads EOF on `/dev/null` stdin and exits cleanly. Add the env var (§4) and `systemctl restart cdp-mcp`. |
| `Connection refused on 127.0.0.1:8000` | Unit not running or crashed — `systemctl status cdp-mcp`, `journalctl -u cdp-mcp`. |
| `ModuleNotFoundError: No module named 'mcp.server.fastmcp'` | Installed lockless (`uv pip install -e`) → got `mcp` 2.0. Reinstall with `uv sync --extra dev` (uses the lock's 1.x pin). |
| `ModuleNotFoundError: No module named 'mcp'` | The editable install didn't complete — re-run `uv sync --extra dev`. |
| `401` + `www-authenticate: negotiate` from a downstream endpoint | The cluster **is** Kerberized — switch to [vps-install.md](vps-install.md). |
| `[Errno 8] nodename nor servname` | Internal-only hostname; you need the §6 autossh tunnel with `socks5h://`. |
| `Connection refused on 127.0.0.1:1080` | autossh tunnel down (only if you enabled §6). |
| `cm_client` 401/403 from CM | Wrong `username`/`password` in `cm_instances.yaml` or `cdp-mcp.env`; check `api_version` matches your CM. |
| Tool returns "endpoint not discovered" | The service wasn't auto-discovered from CM. Check `list_roles`, or set `endpoints_override` in the yaml. |

---

## 9. Operational checklist

- [ ] System packages: `python3.12`, `git`, `gcc` (§1).
- [ ] cdp-mcp installed with `uv sync --extra dev` (§2).
- [ ] `cm_instances.yaml` has `kerberos` off (default), no keytab fields (§3).
- [ ] Secrets in `/etc/cdp-mcp/cdp-mcp.env`, `chmod 600`, owned by `cdp` (§3).
- [ ] `cdp` service user created; owns `/opt/cdp-mcp-server` (§4).
- [ ] `cdp-mcp.service` enabled with `MCP_TRANSPORT=streamable-http`, `MCP_HOST=127.0.0.1`; no `KRB5CCNAME`, no autossh `Requires=` (§4).
- [ ] `curl http://127.0.0.1:8000/mcp` returns an HTTP response (listener up) (§7).
- [ ] (If internal-only) §6 autossh tunnel up + `ALL_PROXY=socks5h://` set (§6).
- [ ] Client reaches the loopback endpoint over an SSH tunnel (§5A).