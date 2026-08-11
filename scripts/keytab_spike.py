#!/usr/bin/env python3
"""
keytab_spike.py — Validate the in-process keytab SPNEGO path end-to-end.

Unlike scripts/spnego_spike.py (which uses a `kinit` TGT in the default ccache),
this script exercises the REAL production keytab path: it calls
`cdp_mcp.clients.spnego.build_spnego_auth(kerberos=True, keytab=…, principal=…)`
— which acquires a TGT in-process via `gssapi.Credentials(usage='initiate',
store={'client_keytab': …, 'ccache': 'MEMORY:cdp-mcp'})` — and uses the returned
auth to make a SPNEGO request to a target endpoint.

This proves, layer by layer:
  1. The [kerberos] extra + MIT krb5 are usable (imports succeed).
  2. The keytab acquires a TGT (gssapi does the AS-REQUEST to the KDC) — the step
     that the previous `store={'keytab': …}` bug silently skipped.
  3. SPNEGO authenticates against the target (HTTP 200).

Usage (on the VPS, with the autossh SOCKS tunnel + the KDC TCP forward up):
  SPNEGO_URL=https://nn-1.cluster.internal:9871/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState \
  SPNEGO_PROXY=socks5h://127.0.0.1:8760 \
  KEYTAB=/etc/cdp-mcp/mcp.keytab \
  PRINCIPAL=mcp-svc@EXAMPLE.CO.ID \
    .venv/bin/python scripts/keytab_spike.py

  # optional: SPNEGO_QRY="Hadoop:service=NameNode,name=NameNodeStatus" to override
  #            (only used when SPNEGO_URL has no query string of its own)

Notes:
  - gssapi does the AS-REQUEST over krb5 (TCP to the KDC), NOT over ALL_PROXY.
    So the KDC must be directly reachable or via a TCP forward (e.g. the
    `kdc = localhost:1088` + `ssh -L 1088:<kdc>:88` setup). ALL_PROXY only
    carries the SPNEGO service request to the NameNode/web endpoint.
  - Exit 0 on SPNEGO OK (HTTP 200), non-zero on any failure.

Exit codes: 0=OK, 2=preflight fail, 3=not SPNEGO-protected, 4=extra missing,
            5=keytab acquisition failed (SpnegoConfigError), 6=non-200/other.
"""
from __future__ import annotations

import os
import subprocess
import sys
from typing import Any


def _step(n: int, msg: str) -> None:
    print(f"\n[{n}] {msg}")


def _preflight(keytab: str, principal: str) -> None:
    _step(1, "Preflight")
    try:
        out = subprocess.run(["klist", "-V"], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        print("  klist not found — install krb5-user (MIT krb5).")
        sys.exit(2)
    print("  " + out.stdout.strip())
    if out.returncode != 0:
        print("  krb5 tooling not working.")
        sys.exit(2)
    if not os.path.isfile(keytab):
        print(f"  keytab not found at {keytab!r}")
        sys.exit(2)
    print(f"  keytab: {keytab}")
    print(f"  principal: {principal}")


def _unauth_probe(base_url: str, qry: str, proxy: str | None) -> None:
    import httpx

    url = base_url if "?" in base_url else f"{base_url}/jmx"
    params = None if "?" in base_url else {"qry": qry}
    _step(2, f"Unauthenticated probe → {url}")
    with httpx.Client(verify=False, follow_redirects=True, timeout=30, proxy=proxy) as c:
        resp = c.get(url, params=params)
    www = resp.headers.get("www-authenticate", "")
    print(f"  status: {resp.status_code}")
    print(f"  www-authenticate: {www!r}")
    if resp.status_code != 401 or "negotiate" not in www.lower():
        print("  Endpoint did NOT challenge with SPNEGO (401 + Negotiate).")
        print(f"  body[:300]: {resp.text[:300]!r}")
        sys.exit(3)
    print("  SPNEGO challenge confirmed.")


def _keytab_spnego_request(
    base_url: str, qry: str, proxy: str | None, keytab: str, principal: str
) -> dict[str, Any]:
    from cdp_mcp.clients.spnego import build_spnego_auth  # the REAL production path

    url = base_url if "?" in base_url else f"{base_url}/jmx"
    params = None if "?" in base_url else {"qry": qry}

    _step(3, "Acquire TGT in-process from keytab (production build_spnego_auth)")
    try:
        auth = build_spnego_auth(kerberos=True, keytab=keytab, principal=principal)
    except Exception as exc:  # SpnegoConfigError or gssapi error
        print(f"  keytab acquisition FAILED: {exc}")
        print("  → verify: kinit -k -t KEYTAB PRINCIPAL succeeds as this user,")
        print("    krb5.conf realm/KDC is correct, and the KDC is reachable")
        print("    (gssapi AS-REQUEST is over krb5, not ALL_PROXY).")
        sys.exit(5)
    print(f"  auth acquired: {auth!r}")

    import httpx

    _step(4, f"SPNEGO-authed request → {url}")
    with httpx.Client(
        base_url="" if "?" in base_url else base_url,
        auth=auth,
        verify=False,
        follow_redirects=True,
        timeout=30,
        proxy=proxy,
    ) as c:
        if "?" in base_url:
            resp = c.get(url)
        else:
            resp = c.get("/jmx", params=params)
    print(f"  status: {resp.status_code}")
    print(f"  content-type: {resp.headers.get('content-type')!r}")
    print(f"  body[:300]: {resp.text[:300]!r}")
    if resp.status_code != 200:
        print("  Authed request did not return 200.")
        sys.exit(6)
    try:
        return resp.json()
    except ValueError as exc:
        print(f"  Response is not JSON: {exc}")
        sys.exit(6)


def main() -> None:
    base_url = (os.environ.get("SPNEGO_URL") or "").rstrip("/")
    qry = os.environ.get("SPNEGO_QRY", "Hadoop:service=NameNode,name=FSNamesystemState")
    proxy = os.environ.get("SPNEGO_PROXY") or None
    keytab = os.environ.get("KEYTAB") or ""
    principal = os.environ.get("PRINCIPAL") or ""
    if not base_url or not keytab or not principal:
        print("Set SPNEGO_URL, KEYTAB, and PRINCIPAL (and optionally SPNEGO_PROXY).")
        sys.exit(1)
    print(f"keytab spike → {base_url}")
    if proxy:
        print(f"via proxy: {proxy}")

    _preflight(keytab, principal)
    _unauth_probe(base_url, qry, proxy)
    data = _keytab_spnego_request(base_url, qry, proxy, keytab, principal)

    _step(5, "RESULT")
    beans = data.get("beans") or []
    print(f"  parsed beans: {len(beans)}")
    print("  RESULT: keytab SPNEGO OK  ✅")


if __name__ == "__main__":
    main()