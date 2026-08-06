#!/usr/bin/env python3
"""
spnego_spike.py — Standalone spike: prove SPNEGO auth works on this Mac against
a Kerberized HDFS NameNode JMX endpoint, using a TGT from `kinit`.

NO changes to the cdp-mcp server. This is a one-off de-risking script for the
SPNEGO support plan (see .claude/plans/parsed-tumbling-perlis.md, Step 1).

What it proves, layer by layer:
  1. A Kerberos TGT exists (klist preflight) — the client env is live.
  2. The endpoint actually requires SPNEGO (unauth probe returns
     401 + WWW-Authenticate: Negotiate) — same detection as
     hdfs_client.py:60-62.
  3. httpx-gssapi authenticates and the endpoint returns 200.
  4. The JMX JSON parses and carries the fields HdfsClient.get_namenode_status
     (hdfs_client.py:86-132) reads — proving the existing client logic would
     work given the auth object.

Usage:
  kinit <user>@<REALM>
  SPNEGO_URL=https://nn-fqdn:9871 \
  SPNEGO_PROXY=socks5://proxy-host:1080 \
    .venv/bin/python scripts/spnego_spike.py
  # optional: SPNEGO_QRY="Hadoop:service=NameNode,name=NameNodeStatus"
  # NOTE: prefer socks5h:// (h = remote DNS) when the Mac can't resolve the
  # cluster's internal hostname — DNS then happens through the proxy.

Exit code 0 on SPNEGO OK, non-zero on failure.
"""
from __future__ import annotations

import os
import subprocess
import sys
from typing import Any

import httpx


def _step(n: int, msg: str) -> None:
    print(f"\n[{n}] {msg}")


def _klist_preflight() -> None:
    """Confirm a TGT exists before we blame gssapi for any auth failure."""
    _step(1, "Kerberos preflight: klist")
    try:
        out = subprocess.run(
            ["klist"], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        print("  klist not found. Install/run kinit first (e.g. kinit user@REALM).")
        sys.exit(2)
    print("  " + out.stdout.replace("\n", "\n  ").rstrip())
    if out.returncode != 0 or "krbtgt" not in out.stdout:
        print("  No TGT found. Run: kinit <user>@<REALM>")
        sys.exit(2)
    print("  TGT present.")


def _unauth_probe(base_url: str, qry: str, proxy: str | None = None) -> None:
    """Hit the endpoint with no auth; expect 401 + Negotiate challenge."""
    _step(2, f"Unauthenticated probe → {base_url}/jmx?qry={qry}")
    with httpx.Client(
        verify=False, follow_redirects=True, timeout=30, proxy=proxy
    ) as c:
        resp = c.get(f"{base_url}/jmx", params={"qry": qry})
    www = resp.headers.get("www-authenticate", "")
    print(f"  status: {resp.status_code}")
    print(f"  www-authenticate: {www!r}")
    if resp.status_code != 401 or "negotiate" not in www.lower():
        print("  Endpoint did NOT challenge with SPNEGO (401 + Negotiate).")
        print("  Either it's not Kerberized, or the URL/transport is wrong.")
        print(f"  body[:300]: {resp.text[:300]!r}")
        sys.exit(3)
    print("  SPNEGO challenge confirmed (matches SpnegoRequiredError detection).")


def _spnego_request(base_url: str, qry: str, proxy: str | None = None) -> dict[str, Any]:
    """Authed request via httpx-gssapi SPNEGOAuth; return parsed JSON."""
    _step(3, "SPNEGO-authed request with httpx_gssapi.HTTPSPNEGOAuth()")
    try:
        from httpx_gssapi import HTTPSPNEGOAuth as _Auth
    except ImportError as exc:
        print(f"  httpx-gssapi not installed: {exc}")
        print("  Install (macOS, MIT krb5 via brew):")
        print("    PKG_CONFIG_PATH=/opt/homebrew/opt/krb5/lib/pkgconfig \\")
        print("      uv pip install httpx-gssapi")
        sys.exit(4)

    with httpx.Client(
        base_url=base_url,
        auth=_Auth(),
        verify=False,
        follow_redirects=True,
        timeout=30,
        proxy=proxy,
    ) as c:
        resp = c.get("/jmx", params={"qry": qry})
    print(f"  status: {resp.status_code}")
    print(f"  content-type: {resp.headers.get('content-type')!r}")
    print(f"  body[:300]: {resp.text[:300]!r}")
    if resp.status_code != 200:
        print("  Authed request did not return 200.")
        sys.exit(5)
    try:
        return resp.json()
    except ValueError as exc:
        print(f"  Response is not JSON: {exc}")
        sys.exit(6)


def _parse_and_compare(data: dict[str, Any]) -> None:
    """Parse beans and print the fields get_namenode_status reads."""
    _step(4, "Parse JMX beans (compare to HdfsClient.get_namenode_status)")
    beans = data.get("beans") or []
    if not beans:
        print("  No beans in response.")
        sys.exit(7)
    print(f"  bean count: {len(beans)}")
    print(f"  first bean name: {beans[0].get('name')!r}")

    fs = next(
        (b for b in beans if b.get("name", "").endswith("FSNamesystemState")),
        beans[0],
    )
    nn = next(
        (b for b in beans if b.get("name", "").endswith("NameNodeStatus")), {}
    )

    cap_total = fs.get("CapacityTotal", 0)
    cap_used = fs.get("CapacityUsed", 0)
    print("  ── fields read by get_namenode_status ──")
    print(f"  CapacityTotal          : {cap_total}")
    print(f"  CapacityUsed           : {cap_used}")
    print(f"  CapacityRemaining      : {fs.get('CapacityRemaining')}")
    print(f"  UnderReplicatedBlocks  : {fs.get('UnderReplicatedBlocks')}")
    print(f"  CorruptBlocks          : {fs.get('CorruptBlocks')}")
    print(f"  MissingBlocks          : {fs.get('MissingBlocks')}")
    print(f"  FSState                : {fs.get('FSState')}")
    print(f"  NameNodeStatus.State   : {nn.get('State')}")
    print(f"  NameNodeStatus.Host    : {nn.get('HostAndPort')}")

    if cap_total:
        pct = round(cap_used / cap_total * 100, 2)
        print(f"  → capacity_used_pct    : {pct}  (computed like the tool)")
    print("  JMX data usable through SPNEGO.")


def main() -> None:
    base_url = (os.environ.get("SPNEGO_URL") or "").rstrip("/")
    qry = os.environ.get(
        "SPNEGO_QRY", "Hadoop:service=NameNode,name=FSNamesystemState"
    )
    proxy = os.environ.get("SPNEGO_PROXY") or None
    if not base_url:
        print("Set SPNEGO_URL (HDFS NN JMX base, e.g. https://nn-fqdn:9871).")
        sys.exit(1)
    print(f"SPNEGO spike → {base_url}  (qry={qry})")
    if proxy:
        print(f"via proxy: {proxy}")

    _klist_preflight()
    _unauth_probe(base_url, qry, proxy)
    data = _spnego_request(base_url, qry, proxy)
    _parse_and_compare(data)

    _step(5, "RESULT")
    print("  RESULT: SPNEGO OK  ✅")


if __name__ == "__main__":
    main()