# Installing cdp-mcp on a VPS (with autossh SPNEGO support)

This guide installs cdp-mcp as an **unattended, long-running daemon on a Linux
VPS** that reaches a Kerberized CDP cluster sitting behind a jump host. It wires
together three persistent pieces:

1. **`cdp-mcp`** — the MCP server (systemd service).
2. **`autossh`** — a self-healing SOCKS5 tunnel to the jump host, so the
   cluster's internal-only hostnames resolve and connect from the VPS
   (systemd service).
3. **Kerberos credentials** — a keytab loaded into the default ccache, kept
   alive by a cron/systemd renewal so SPNEGO never expires.

For the *concepts* behind SOCKS5h and `kinit` (why remote DNS matters, how
`HTTPSPNEGOAuth` reads the ccache), read [`kerberos-tunneling.md`](./kerberos-tunneling.md)
first. This doc is the ops playbook that turns those concepts into systemd units.

> CM itself always uses Basic auth and is **never** proxied by Kerberos. The
> `autossh` tunnel + keytab affect **only** the four downstream service clients
> (NameNode JMX, YARN RM, Spark HS, Oozie) when a CM instance has `kerberos: true`.

---

## 0. Assumptions

- VPS: a modern RHEL/Rocky/Alma or Debian/Ubuntu box with root/sudo.
- The VPS can SSH to a **jump host** that itself can reach the cluster's
  internal hostnames. The VPS generally *cannot* reach them directly.
- You have a Kerberos **keytab** for a service principal (e.g. `mcp-svc@REALM`).
- You already have a working `cm_instances.yaml` (see
  [`installation.md`](./installation.md)).

If your cluster is directly reachable from the VPS and **not** Kerberized, you
don't need this guide at all — use [VPS install (no Kerberos)](vps-install-no-kerberos.md)
instead (no autossh, no keytab, no `kinit`).

---

## 1. System packages

### RHEL / Rocky / Alma

```bash
sudo dnf install -y \
  git python3.12 python3.12-devel krb5-workstation krb5-devel \
  gcc make autossh
```

### Debian / Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y \
  git python3.12 python3.12-dev python3.12-venv \
  krb5-user libkrb5-dev gcc make autossh
```

Verify:

```bash
python3.12 --version
klist -V          # krb5 tooling present
autossh -V        # autossh present
```

### Install uv (used to build the venv + lockfile)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# add to PATH per the installer's output, or:
export PATH="$HOME/.local/bin:$PATH"
```

---

## 2. Kerberos keytab + renewal (SPNEGO credentials)

`HTTPSPNEGOAuth` reads credentials either from the **default credentials cache**
or — when `kerberos_keytab`/`kerberos_principal` are set on the CM instance —
**directly from a keytab in-process**. Two strategies; pick one:

- **B2 — in-process keytab (recommended for a VPS):** set `kerberos_keytab` +
  `kerberos_principal`. cdp-mcp acquires a fresh TGT from the keytab on every
  downstream tool call via gssapi, so **no external `kinit` and no renewal
  timer are needed.** Skip §2.3–2.4 entirely and just place the keytab (§2.1) and
  configure the realm (§2.2). Then set the two fields in `cm_instances.yaml`
  (§3.1).
- **B1 — keytab loaded into the ccache + renewal timer:** the original path
  below. A systemd timer re-runs `kinit` from the keytab every 8h. Use this if
  you want to avoid the per-tool-call KDC AS-REQ that B2 incurs.

> The keytab file is a credential equivalent to a password — `chmod 600`,
> owned by root (or the service user). **Never commit it to the repo.**

### 2.1 Place the keytab (both B1 and B2)

```bash
sudo mkdir -p /etc/cdp-mcp
sudo install -o root -g root -m 600 mcp.keytab /etc/cdp-mcp/mcp.keytab
```

### 2.2 Configure the realm

Ensure `/etc/krb5.conf` points at your KDC, e.g.:

```ini
[libdefaults]
    default_realm = CLOUD.INTRA.EXAMPLE.CO.ID
    dns_lookup_realm = false
    dns_lookup_kdc = false

[realms]
    CLOUD.INTRA.EXAMPLE.CO.ID = {
        kdc = kdc1.intra.example.co.id
        admin_server = kdc1.intra.example.co.id
    }

[domain_realm]
    .intra.example.co.id = CLOUD.INTRA.EXAMPLE.CO.ID
    intra.example.co.id = CLOUD.INTRA.EXAMPLE.CO.ID
```

### 2.3 Acquire the initial TGT from the keytab — B1 only

> **B2 (in-process keytab) users skip §2.3 and §2.4** — cdp-mcp reads the keytab
> directly, no ccache or renewer needed. Still create the `cdp` service user
> (used by the systemd units); just don't run `kinit`.

The `cdp-mcp` service will run as a dedicated user `cdp`. Create it, then load
the keytab into that user's default ccache:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin cdp

sudo -u cdp KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u cdp) \
  kinit -k -t /etc/cdp-mcp/mcp.keytab mcp-svc@CLOUD.INTRA.EXAMPLE.CO.ID

sudo -u cdp KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u cdp) klist
```

> Pick **one** `KRB5CCNAME` path and use it everywhere: the initial `kinit`,
> the renewal job, and the `cdp-mcp` service `Environment=`. If they disagree,
> the server reads an empty/expired cache and SPNEGO fails with
> `spnego_config_error: no usable Kerberos credentials`.

### 2.4 Keep the TGT alive (systemd timer) — B1 only

A TGT typically expires in ~10h. Renew it well before that. A systemd timer is
cleaner than cron on a modern VPS:

`/etc/systemd/system/cdp-mcp-kinit.service`

```ini
[Unit]
Description=Refresh cdp-mcp Kerberos TGT from keytab

[Service]
Type=oneshot
User=cdp
Environment=KRB5CCNAME=FILE:/tmp/krb5cc_1000
ExecStart=/usr/bin/kinit -k -t /etc/cdp-mcp/mcp.keytab mcp-svc@CLOUD.INTRA.EXAMPLE.CO.ID
```

`/etc/systemd/system/cdp-mcp-kinit.timer`

```ini
[Unit]
Description=Refresh cdp-mcp Kerberos TGT every 8h

[Timer]
OnBootSec=1min
OnUnitActiveSec=8h
Persistent=true

[Install]
WantedBy=timers.target
```

> Replace `1000` with `$(id -u cdp)` and the principal with yours.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cdp-mcp-kinit.timer
systemctl list-timers cdp-mcp-kinit.timer
```

The cached `HTTPSPNEGOAuth` re-reads the ccache on every handshake, so renewal
takes effect **without restarting cdp-mcp**.

---

## 3. Install cdp-mcp

```bash
sudo -u cdp -i   # or run the clone as a user that owns the install dir

cd /opt
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server

uv venv --python 3.12
# Install from uv.lock (base + dev + the [kerberos] extra). Use `uv sync`, NOT
# `uv pip install -e '.[...]'`: the latter resolves fresh and ignores uv.lock,
# which can pull an incompatible major version of `mcp` (2.0 removed the
# `mcp.server.fastmcp` import server.py uses). uv.lock pins the known-good 1.x.
# The [kerberos] extra's `gssapi` is a native build — needs the krb5/headers
# from §1, else this step fails on the C compiler.
uv sync --extra dev --extra kerberos

# Confirm the SPNEGO path imports
.venv/bin/python -c "import httpx_gssapi; print('httpx-gssapi OK')"
.venv/bin/cdp-mcp --help || true
```

`httpx[socks]` (the `socksio` package) is already a base dependency, so the
SOCKS5h proxy needs no extra install step.

### 3.1 Registry config

```bash
cp cm_instances.yaml.example cm_instances.yaml
```

Edit `cm_instances.yaml` — set `kerberos: true` on the instance that owns the
Kerberized cluster. For **B2 (in-process keytab)**, also set the keytab fields
so cdp-mcp acquires the TGT itself (no renewal timer). For **B1**, leave them
out and rely on the §2.3–2.4 ccache+timer.

```yaml
instances:
  - host: cm.example.com
    port: 7183
    username: admin
    password: "${CM_PASSWORD}"
    environment_name: prod
    use_tls: true
    verify_ssl: false
    api_version: v51
    timeout_seconds: 30
    downstream_timeout_seconds: 30
    kerberos: true          # attach SPNEGO to the downstream clients
    disable_on_spnego: true
    # B2 (in-process keytab) — set these to skip the §2.3–2.4 ccache/timer:
    kerberos_keytab: /etc/cdp-mcp/mcp.keytab
    kerberos_principal: mcp-svc@CLOUD.INTRA.EXAMPLE.CO.ID
    active: true
```

> With B2, you can drop the `KRB5CCNAME` line from the [§5](#5-cdp-mcp-systemd-service)
> unit and the `cdp-mcp-kinit` timer becomes unnecessary.

Put the secret in `/etc/cdp-mcp/cdp-mcp.env` (gitignored, `chmod 600`):

```bash
sudo install -m 600 /dev/null /etc/cdp-mcp/cdp-mcp.env
sudo tee -a /etc/cdp-mcp/cdp-mcp.env >/dev/null <<'EOF'
CM_PASSWORD=changeme
EOF
sudo chown cdp:cdp /etc/cdp-mcp/cdp-mcp.env
```

> Outbound proxy is **not** a yaml field — it's always the `ALL_PROXY` env var,
> set on the service unit in [§5](#5-cdp-mcp-systemd-service).

---

## 4. autossh SOCKS tunnel (network reachability)

The VPS can't resolve/connect the cluster's internal hostnames directly, so we
forward every HTTP request through a SOCKS5 proxy on the jump host. `autossh`
reconnects the SSH session when it drops; systemd keeps `autossh` itself alive.

### 4.1 SSH key for the jump host (no password, no agent)

```bash
sudo -u cdp ssh-keygen -t ed25519 -N "" -f /var/lib/cdp/.ssh/jumphost_ed25519
sudo -u cdp ssh-copy-id -i /var/lib/cdp/.ssh/jumphost_ed25519.pub jumphost-user@jumphost.example.com
```

Pin the jump host key to avoid `StrictHostKeyChecking` prompts in a daemon:

```bash
sudo -u cdp ssh-keyscan -H jumphost.example.com >> /var/lib/cdp/.ssh/known_hosts
```

### 4.2 autossh systemd service

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

Key flags:

- `-M 0` — disable autossh's legacy monitoring port; rely on SSH's own
  `ServerAlive*` keepalives instead (the modern, robust choice).
- `-D 127.0.0.1:1080` — bind the SOCKS5 proxy to localhost only (the VPS is the
  only consumer; no need to expose it).
- `ExitOnForwardFailure=yes` — if the dynamic forward can't be established, the
  process exits and systemd restarts it, rather than running a useless session.
- `ServerAliveInterval=30` / `ServerAliveCountMax=3` — detect a dead peer within
  ~90s and trigger reconnect.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cdp-mcp-autossh.service
ss -ltnp | grep 1080     # confirm the SOCKS listener is up
```

### 4.3 Verify the tunnel (no Kerberos yet)

```bash
sudo -u cdp ALL_PROXY=socks5h://127.0.0.1:1080 \
  curl -k -s -o /dev/null -w "%{http_code}\n" \
  https://nn-1.cluster.internal:9871/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
```

A `401` with `WWW-Authenticate: Negotiate` means the proxy reaches the
NameNode — network layer good. A connection error means the tunnel isn't up;
fix that before continuing. Always use `socks5h://` (remote DNS), never
`socks5://` — internal hostnames don't resolve on the VPS.

---

## 5. cdp-mcp systemd service

`/etc/systemd/system/cdp-mcp.service`

```ini
[Unit]
Description=cdp-mcp MCP server
After=network-online.target cdp-mcp-autossh.service cdp-mcp-kinit.timer
Wants=network-online.target
Requires=cdp-mcp-autossh.service

[Service]
Type=simple
User=cdp
WorkingDirectory=/opt/cdp-mcp-server

# Run as a long-lived HTTP daemon (NOT stdio). A stdio server under systemd reads
# EOF on /dev/null stdin and exits cleanly right after startup — useless as a
# service. streamable-http keeps a uvicorn listener up; clients connect to
# http://<host>:8000/mcp. Bind to loopback and reach it over an SSH tunnel
# (§6) — the HTTP transport has NO built-in auth, so never set MCP_HOST=0.0.0.0
# on a public VPS without a reverse proxy / firewall in front.
Environment=MCP_TRANSPORT=streamable-http
Environment=MCP_HOST=127.0.0.1
Environment=MCP_PORT=8000
# Shared-secret bearer gate (recommended): generate a secret with
#   python -c "import secrets; print(secrets.token_urlsafe(32))"
# and set it here. Clients must then send `Authorization: Bearer <secret>` or
# they get 401. A reverse proxy in front may set/forward that header.
#Environment=MCP_AUTH_TOKEN=<your-secret>

# Kerberos ccache (B1 only) — MUST match the kinit service + timer (§2.3/§2.4).
# B2 (in-process keytab) does not use the ccache: drop this line.
Environment=KRB5CCNAME=FILE:/tmp/krb5cc_1000

# SOCKS5h proxy through the autossh tunnel (remote DNS for internal hostnames)
Environment=ALL_PROXY=socks5h://127.0.0.1:1080

# Registry
Environment=REGISTRY_BACKEND=file
Environment=REGISTRY_FILE_PATH=/opt/cdp-mcp-server/cm_instances.yaml

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

Notes:

- `MCP_TRANSPORT=streamable-http` is what makes the unit a real daemon — without
  it cdp-mcp defaults to stdio and exits immediately (stdin is `/dev/null` under
  systemd → EOF → clean exit 0). `MCP_HOST=127.0.0.1` keeps it loopback-only;
  see [§6](#6-point-your-mcp-client-at-the-vps) for reaching it over an SSH
  tunnel. **Security:** no auth on this endpoint — never expose it publicly.
- `Requires=cdp-mcp-autossh.service` — don't start cdp-mcp without the tunnel.
  (The kinit *timer* isn't a `Requires` because it's a oneshot, not a daemon;
  the ordering `After=` is enough.)
- `KRB5CCNAME` here **must** equal the path used in [§2.3](#23-acquire-the-initial-tgt-from-the-keytab)
  and [§2.4](#24-keep-the-tgt-alive-systemd-timer), or SPNEGO reads the wrong
  (empty) cache. **B2 users drop this line** — the keytab is the credential
  source, not the ccache.
- `ALL_PROXY=socks5h://` — the `h` is mandatory for remote DNS. This covers both
  the CM client and the four downstream clients (httpx `trust_env` is on by
  default; no code/config field needed).
- If you'd rather log to a file, set `MCP_LOG_LEVEL`/add a logging config; by
  default cdp-mcp logs to stdout, which systemd captures in the journal.

Start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cdp-mcp.service
sudo journalctl -u cdp-mcp -f
```

The server now stays up. You should see `cdp_mcp.transport transport=streamable-http`,
`cdp_mcp.ready`, and (on the first Kerberized downstream call) `spnego.auth_initialized
source=ccache` (B1) or `source=keytab` (B2). `Restart=on-failure` brings it back
if uvicorn crashes.

---

## 6. Point your MCP client at the VPS

The [§5](#5-cdp-mcp-systemd-service) unit runs cdp-mcp as an HTTP server on
`127.0.0.1:8000` (path `/mcp`). Because it's bound to loopback with no auth, you
reach it over an SSH tunnel from the laptop (the tunnel is the only thing that
needs to touch the VPS network).

### A. Laptop client via an SSH tunnel (recommended)

Open a local forward to the VPS's cdp-mcp port (run this on the laptop; keep it
open while you use the client, or set up an autossh tunnel of your own):

```bash
ssh -L 8000:127.0.0.1:8000 -N cdp@vps.example.com
```

Then point a streamable-HTTP-capable MCP client at the local end of the tunnel:

```json
{
  "mcpServers": {
    "cdp": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

> If you set `MCP_AUTH_TOKEN` on the unit, the client must send the bearer token,
> e.g. via a `headers` field (supported by streamable-HTTP-capable MCP clients):
> ```json
> { "mcpServers": { "cdp": { "url": "http://127.0.0.1:8000/mcp",
>   "headers": { "Authorization": "Bearer <your-secret>" } } } }
> ```
> Without it, the server returns 401. (With the MCP Inspector, set the header in
> the transport config; with curl, add `-H 'Authorization: Bearer <your-secret>'`.)

The laptop needs no Kerberos, no keytab, no SOCKS proxy — all of that lives on the
VPS inside the systemd unit. The SSH tunnel is the auth/transport boundary.

### B. Client on the VPS itself

If the MCP client runs on the VPS, it can hit the loopback endpoint directly:

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

If you'd rather not run the persistent HTTP server, skip [§5](#5-cdp-mcp-systemd-service)
entirely and let the client spawn cdp-mcp per session over stdio. The autossh
tunnel + keytab (their own units) still run; the laptop's client SSHes in and
launches cdp-mcp, which inherits the VPS env via the `cdp` user's shell profile
(set `ALL_PROXY`/`KRB5CCNAME` there, or pass them in the ssh command):

```json
{
  "mcpServers": {
    "cdp": {
      "command": "ssh",
      "args": ["cdp@vps.example.com",
               "ALL_PROXY=socks5h://127.0.0.1:1080 KRB5CCNAME=FILE:/tmp/krb5cc_1000 /opt/cdp-mcp-server/.venv/bin/cdp-mcp"]
    }
  }
}
```

This is per-session (a fresh process each time); the HTTP daemon in §5 is one
always-on process many clients can share.

---

## 7. End-to-end verification

```bash
# 1. Tunnel up?
sudo systemctl is-active cdp-mcp-autossh
ss -ltnp | grep 1080

# 2. TGT valid? (B1 only — B2 has no ccache; the server acquires from the keytab)
sudo -u cdp KRB5CCNAME=FILE:/tmp/krb5cc_1000 klist

# 3. HTTP daemon up + listening on loopback?
sudo systemctl is-active cdp-mcp
ss -ltnp | grep ':8000'
sudo journalctl -u cdp-mcp --since "5 min ago" | grep -E 'transport|ready|spnego'
#    expect: cdp_mcp.transport transport=streamable-http  ...  cdp_mcp.ready
#    and on the first Kerberized downstream call:
#      B1 → "source=ccache"; B2 → "source=keytab principal=... keytab=..."

# 3b. Local reachability of the MCP endpoint (from the VPS):
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/mcp
#    expect 406/405/200 (any HTTP response proves the listener is up; a
#    connection refused means the unit isn't running or isn't bound).

# 4. Run the bundled spike script through the tunnel+ccache (B1).
#    The spike uses the default ccache, so it's a B1 check; for B2, verify by
#    calling a downstream MCP tool (e.g. get_namenode_status) and watching the
#    journal for "source=keytab".
sudo -u cdp KRB5CCNAME=FILE:/tmp/krb5cc_1000 \
  ALL_PROXY=socks5h://127.0.0.1:1080 \
  SPNEGO_URL=https://nn-1.cluster.internal:9871 \
  SPNEGO_PROXY=socks5h://127.0.0.1:1080 \
  /opt/cdp-mcp-server/.venv/bin/python /opt/cdp-mcp-server/scripts/spnego_spike.py
```

On an unfamiliar cluster, the first MCP tool to call is
`get_cluster_security_info` (confirm TLS/Kerberos status), then `list_roles`
to see the service URLs auto-discovery will use.

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `cdp-mcp.service` exits 0 right after `cdp_mcp.ready` ("Deactivated successfully") | Missing `MCP_TRANSPORT=streamable-http` on the unit → defaults to stdio → reads EOF on `/dev/null` stdin and exits cleanly. Add the env var (§5) and `systemctl restart cdp-mcp`. |
| `Connection refused on 127.0.0.1:8000` | Unit not running or crashed — `systemctl status cdp-mcp`, `journalctl -u cdp-mcp`. |
| `[Errno 8] nodename nor servname` | Used `socks5://` — switch to `socks5h://` (remote DNS). |
| `Connection refused on 127.0.0.1:1080` | autossh tunnel down — `systemctl status cdp-mcp-autossh`, check `IdentityFile`/known_hosts. |
| `spnego_config_error: httpx-gssapi not installed` | Reinstall with `[kerberos]` extra (§3); confirm `python -c "import httpx_gssapi"`. |
| `spnego_config_error: no usable Kerberos credentials` | **B1:** ccache empty/expired or `KRB5CCNAME` mismatch between kinit-timer and the cdp-mcp unit — run `klist` as the `cdp` user with the unit's `KRB5CCNAME`. |
| `spnego_config_error: ...keytab acquisition failed` (B2) | Wrong principal in the keytab, realm/KDC misconfigured in `krb5.conf`, or KDC unreachable through the tunnel. Verify with `kinit -k -t /etc/cdp-mcp/mcp.keytab mcp-svc@REALM` (must succeed). |
| `spnego_config_error: ...no principal was configured` (B2) | `kerberos_keytab` set without `kerberos_principal` — add the principal to the instance. |
| `spnego_config_error: ...keytab not found` (B2) | `kerberos_keytab` path wrong or unreadable by the `cdp` user — check `chmod 600` + ownership. |
| `401` persists, SPNEGO never completes | **B1:** no valid TGT — check `klist`; trigger `systemctl start cdp-mcp-kinit`. **B2:** check the `source=keytab` log line + the acquisition error above. |
| Worked yesterday, fails today | TGT expired — timer misfired; run `systemctl start cdp-mcp-kinit` and verify the timer (`systemctl list-timers`). |
| SPNEGO works for one service, fails for another | Different service principals / per-host SOCKS reachability — verify each URL with the spike script. |
| autossh won't stay up | `ExitOnForwardFailure=yes` + bad jump host key/identity → immediate exit. Check `journalctl -u cdp-mcp-autossh`. |

---

## 9. Operational checklist

- [ ] System packages: `python3.12`, `krb5-*`, `autossh`, `gcc` (§1).
- [ ] Keytab at `/etc/cdp-mcp/mcp.keytab`, `chmod 600` (§2.1).
- [ ] `/etc/krb5.conf` realm + KDC correct (§2.2).
- [ ] **B1 only:** initial `kinit` succeeds as `cdp` with the chosen `KRB5CCNAME` (§2.3).
- [ ] **B1 only:** `cdp-mcp-kinit.timer` enabled, fires every 8h (§2.4).
- [ ] **B2 only:** `cm_instances.yaml` sets `kerberos_keytab` + `kerberos_principal` (§3.1); no kinit/timer.
- [ ] cdp-mcp installed with `[kerberos]` extra; `import httpx_gssapi` works (§3).
- [ ] `cm_instances.yaml` has `kerberos: true` on the Kerberized instance (§3.1).
- [ ] Secrets in `/etc/cdp-mcp/cdp-mcp.env`, `chmod 600`, owned by `cdp` (§3.1).
- [ ] Jump host SSH key + known_hosts pinned (§4.1).
- [ ] `cdp-mcp-autossh.service` enabled, listening on `127.0.0.1:1080` (§4.2).
- [ ] Tunnel verify: `curl` through `socks5h://` returns `401 Negotiate` (§4.3).
- [ ] `cdp-mcp.service` enabled with `MCP_TRANSPORT=streamable-http`, `MCP_HOST=127.0.0.1`; `ALL_PROXY` set; `KRB5CCNAME` set only for B1 (§5).
- [ ] `curl http://127.0.0.1:8000/mcp` returns an HTTP response (listener up) (§7).
- [ ] Client reaches the loopback endpoint over an SSH tunnel (§6A).