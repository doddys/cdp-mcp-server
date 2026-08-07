# Accessing a Kerberized cluster behind a jump host — SOCKS + kinit setup

This guide covers the two things you need before the SPNEGO-enabled downstream
clients (YARN RM, Spark History Server, HDFS NameNode JMX, Oozie) can reach a
Kerberized CDP cluster that has no direct network path from your workstation:

1. **SOCKS tunneling** — route every HTTP request through an SSH dynamic port
   forward so the cluster's internal-only hostnames resolve and connect.
2. **Kerberos credentials (`kinit`)** — populate the default credentials cache
   with a TGT so `HTTPSPNEGOAuth` can attach SPNEGO to those requests.

These two layers are independent but usually both required together: the SOCKS
proxy gets bytes to the host; the Kerberos ticket gets you past the
`401 Negotiate` challenge once you're there.

> CM itself always uses Basic auth and is **never** affected by either layer.
> `kerberos`/`ALL_PROXY` apply only to the four downstream service clients.

---

## 1. SOCKS tunneling (network reachability)

### Why SOCKS5h specifically

On a typical CDP cluster the service URLs are internal hostnames
(e.g. `nn-1.cluster.internal`) that **do not resolve on your laptop**. You need
DNS resolution to happen *on the proxy side*, which is what the `h` in
`socks5h://` means. Plain `socks5://` resolves locally and fails with
`[Errno 8] nodename nor servname provided, or not known` for these hostnames.
This was confirmed live during the SPNEGO spike.

### Step 1 — open an SSH dynamic port forward

From a host that *can* reach the cluster (your jump host / bastion), open a
SOCKS5 proxy on a local port:

```bash
ssh -D 1080 -N -C user@jumphost.example.com
```

- `-D 1080` — listen on `localhost:1080` as a SOCKS5 proxy.
- `-N` — no remote command (just forward).
- `-C` — compress (optional, helps on slow links).

Keep this shell open while cdp-mcp is running. To run it in the background and
have it reconnect on drops, use `autossh`:

```bash
autossh -M 0 -D 1080 -N \
  -o "ServerAliveInterval=30" -o "ServerAliveCountMax=3" \
  user@jumphost.example.com
```

### Step 2 — point cdp-mcp at the proxy

Every HTTP client in cdp-mcp honors the standard proxy env vars because
httpx's `trust_env` is on by default — no code or config field needed:

```bash
ALL_PROXY=socks5h://127.0.0.1:1080 REGISTRY_BACKEND=file cdp-mcp
```

- `ALL_PROXY` covers both the CM client and the four downstream clients.
- `HTTPS_PROXY` also works and takes precedence for HTTPS URLs; `ALL_PROXY` is
  the simplest single-var choice.
- **Always use `socks5h://`**, not `socks5://`, for the remote-DNS reason above.

The `httpx[socks]` extra (the `socksio` package) is already a declared
dependency — `uv sync --extra dev` pulls it in automatically. No extra install
step.

### Step 3 — verify the tunnel

Quick reachability check before starting the server (no Kerberos yet):

```bash
# Should return 401 + WWW-Authenticate: Negotiate (proves the proxy reaches NN)
ALL_PROXY=socks5h://127.0.0.1:1080 \
  curl -k -s -o /dev/null -w "%{http_code}\n" \
  https://nn-1.cluster.internal:9871/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState
```

If you get a connection error here, the tunnel isn't up or the hostname is
wrong — fix that before moving on to Kerberos. The bundled spike script also
walks this layer by layer:

```bash
SPNEGO_URL=https://nn-1.cluster.internal:9871 \
SPNEGO_PROXY=socks5h://127.0.0.1:1080 \
  .venv/bin/python scripts/spnego_spike.py
```

---

## 2. Kerberos credentials (`kinit` tunneling)

SPNEGO auth uses the **default Kerberos credentials cache** (ccache) — whatever
`kinit` (or a keytab loader) put there. `build_spnego_auth()` in
`clients/spnego.py` builds one `HTTPSPNEGOAuth` per process that reads the
ccache on every handshake, so a renewed TGT is picked up without restarting
the server.

### Install prerequisite (one-time)

The SPNEGO code path is an optional extra so non-Kerberized users don't need
kr5 libs. Install it (needs MIT krb5 at build/runtime). Use `uv sync` so it
reads from `uv.lock` (which pins the kerberos extra); a lockless
`uv pip install -e '.[kerberos]'` can resolve to an incompatible major `mcp`:

```bash
# macOS (brew krb5)
PKG_CONFIG_PATH=/opt/homebrew/opt/krb5/lib/pkgconfig \
  uv sync --extra kerberos

# RHEL/Rocky
sudo dnf install -y krb5-devel && uv sync --extra kerberos

# Debian/Ubuntu
sudo apt-get install -y libkrb5-dev && uv sync --extra kerberos
```

Confirm the libraries are visible:

```bash
klist -V        # krb5 tooling present
```

### Option A — interactive TGT (laptop / dev)

Simplest path. Get a ticket-granting ticket from your KDC:

```bash
kinit <user>@<REALM>
# e.g. kinit expc_doddy@CLOUD.INTRA.EXAMPLE.CO.ID

klist          # confirm: should show a krbtgt/<REALM>@<REALM> entry
```

The ticket expires (typically 10h) and must be renewed for a long-running
server — see "Renewal" below. Good for ad-hoc troubleshooting from your laptop.

### Option B — keytab (daemon / unattended)

For a long-running cdp-mcp process you don't want interactive `kinit`. There are
two keytab strategies; pick based on where you want the renewal to live.

#### B1 — load the keytab into the ccache (external renewal)

Run `kinit` once from the keytab, then keep the ccache alive with a renewer:

```bash
kinit -k -t /etc/cdp-mcp/mcp.keytab mcp-svc@<REALM>
klist          # confirms the keytab principal is now in the default ccache
```

Notes:
- This still writes to the **default** ccache (`KRB5CCNAME` unset, or set to the
  default `FILE:/tmp/krb5cc_<uid>`). The server reads exactly that ccache.
- The ccache expires (typically 10h), so an external mechanism — cron `kinit -R`,
  or a systemd timer re-running `kinit -k -t ...` — must keep it valid.
- Protect the keytab file (`chmod 600`, owned by the server user). It is a
  credential equivalent to a password — never commit it to the repo.

#### B2 — in-process keytab acquisition (no external renewal — recommended for VPS)

Set `kerberos_keytab` + `kerberos_principal` on the CM instance. cdp-mcp then
acquires a TGT directly from the keytab via gssapi on each downstream tool call,
so **no external `kinit` or renewal cron/timer is needed** — gssapi re-reads the
keytab every time, transparently re-acquiring an expired TGT.

`cm_instances.yaml`:

```yaml
- host: cm.example.com
  ...
  kerberos: true
  kerberos_keytab: /etc/cdp-mcp/mcp.keytab
  kerberos_principal: mcp-svc@<REALM>
```

EnvRegistry:

```bash
CM_KERBEROS=true \
CM_KERBEROS_KEYTAB=/etc/cdp-mcp/mcp.keytab \
CM_KERBEROS_PRINCIPAL=mcp-svc@<REALM> \
  cdp-mcp
```

Notes:
- Requires the `[kerberos]` extra (it pulls in `gssapi`, which provides the
  cred-store keytab acquisition). The base install is not enough.
- One KDC AS-REQ per Kerberized downstream tool call (the client factories build
  a fresh auth per call). Fine for a read-only troubleshooting server; if you
  drive it very hard, B1 with a long-lived renewed ccache avoids the per-call
  AS-REQ.
- `KRB5CCNAME` is irrelevant in this mode — the keytab is the source of truth.
- Same keytab-file protection rules as B1.

### Renewal (long-running server)

Because auth reads the default ccache, an expired TGT makes every downstream
SPNEGO call fail with a `spnego_config_error`. Keep the ccache alive:

```bash
# Interactive / forwardable ticket — renew without re-entering a password:
kinit -R

# Unattended — cron every 8h (well before the 10h expiry):
# /etc/cron.d/cdp-mcp-krb5
0 */8 * * * mcp-svc KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u mcp-svc) \
  kinit -k -t /etc/cdp-mcp/mcp.keytab mcp-svc@<REALM >/dev/null 2>&1
```

Set `KRB5CCNAME` consistently for the server process and the renewer so they
point at the same ccache. The cached `HTTPSPNEGOAuth` reads the ccache per
handshake, so renewal takes effect without restarting cdp-mcp.

---

## 3. Putting it together

```bash
# 1. Tunnel
ssh -D 1080 -N -C user@jumphost.example.com &

# 2. Ticket
kinit <user>@<REALM>

# 3. Run (kerberos flag on the CM instance that owns the cluster)
ALL_PROXY=socks5h://127.0.0.1:1080 \
REGISTRY_BACKEND=env CM_HOST=cm.example.com CM_USERNAME=admin CM_PASSWORD=... \
  CM_KERBEROS=true cdp-mcp
```

Equivalent FileRegistry snippet (`cm_instances.yaml`):

```yaml
instances:
  - host: cm.example.com
    port: 7183
    username: admin
    password: "${CM_PASSWORD}"
    use_tls: false
    kerberos: true          # attach SPNEGO to the downstream clients
    disable_on_spnego: true
```

(Outbound proxy is **not** a yaml field — it's always the `ALL_PROXY` env var.)

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `[Errno 8] nodename nor servname` | Used `socks5://` — switch to `socks5h://` (remote DNS). |
| Connection refused on `127.0.0.1:1080` | SSH `-D` tunnel isn't up. Restart it. |
| `401` persists with `www-authenticate: negotiate` but SPNEGO never succeeds | No valid TGT — run `klist`; if empty/expired, `kinit` / `kinit -R`. |
| `spnego_config_error: httpx-gssapi not installed` | Run the `[kerberos]` extra install (Step 1 of §2). |
| `spnego_config_error: no usable Kerberos credentials` | Ccache empty or expired — same fix as the 401 case. |
| SPNEGO works for one service, fails for another | Different service principals/SOCKS reachability per host — verify each service URL with the spike script. |
| Worked yesterday, fails today | TGT expired — renew (`kinit -R`) or set up cron renewal. |

First call on an unfamiliar cluster should be `get_cluster_security_info` to
confirm its TLS/Kerberos status upfront, then `list_roles` to find the service
URLs auto-discovery will use.