"""
spnego.py — Lazy factory for the SPNEGO auth object attached to the four
downstream service clients (NameNode/YARN/Spark/Oozie) on Kerberized clusters.

Design notes
------------
- `httpx-gssapi` (and its native `gssapi` build dependency) is an OPTIONAL
  install: non-Kerberized users never need krb5 libs. So the import is lazy —
  it only happens when a CM instance has `kerberos=true`. The package is
  declared as a `[kerberos]` extra in pyproject.toml.
- The auth object uses the **default Kerberos credentials cache**: a TGT
  obtained via `kinit`, or a keytab loaded into the ccache. In-process keytab
  acquisition (gssapi.Credentials(keytab=...)) is deferred — see the keytab
  spike to-do. For a long-running server the operator must keep the ccache
  valid (cron `kinit -R` / a keytab-renewed TGT).
- Only the downstream clients get this auth. cm_client.py always uses Basic
  auth and is never modified here.
- Never throws an untyped exception toward server.py (architectural rule #2):
  import/credential failures raise the typed `SpnegoConfigError`.
"""
from __future__ import annotations

from typing import Any

import structlog

from cdp_mcp.clients.errors import SpnegoConfigError

log = structlog.get_logger(__name__)

# Cached auth object per process; HTTPSPNEGOAuth is stateless w.r.t. the ccache
# and safe to share across requests/clients. gssapi reads the ccache per
# handshake, so a renewed TGT is picked up without rebuilding this.
_auth: Any | None = None
_auth_built = False


def build_spnego_auth(kerberos: bool) -> Any:
    """Return an httpx Auth object for SPNEGO, or None when Kerberos is off.

    Args:
        kerberos: the CM instance's `kerberos` flag.

    Returns:
        An `httpx_gssapi.HTTPSPNEGOAuth` instance when kerberos=True, else None.

    Raises:
        SpnegoConfigError: if kerberos=True but httpx-gssapi is not installed
            or the credentials cache has no usable TGT.
    """
    if not kerberos:
        return None

    global _auth, _auth_built
    if _auth_built:
        return _auth

    try:
        from httpx_gssapi import HTTPSPNEGOAuth
    except ImportError as exc:
        raise SpnegoConfigError(
            "Kerberos/SPNEGO is enabled (kerberos=true) but the optional "
            "`httpx-gssapi` package is not installed. Install it with the "
            "[kerberos] extra (e.g. `uv pip install -e '.[kerberos]'`) and "
            "ensure MIT krb5 is available. On macOS build against brew krb5: "
            "PKG_CONFIG_PATH=/opt/homebrew/opt/krb5/lib/pkgconfig."
        ) from exc

    # HTTPSPNEGOAuth() with no args uses the default credentials cache (ccache),
    # i.e. whatever `kinit` put there. mutual=REQUIRED would reject servers that
    # don't complete mutual auth; Hadoop SPNEGO is one-way, so leave default
    # (mutual=DISABLED/OPTIONAL per httpx_gssapi defaults).
    try:
        _auth = HTTPSPNEGOAuth()
    except Exception as exc:  # gssapi can raise on a missing/bad ccache
        raise SpnegoConfigError(
            "Kerberos/SPNEGO is enabled but no usable Kerberos credentials were "
            "found in the default cache. Obtain a TGT (e.g. `kinit user@REALM`) "
            f"or load a keytab into the ccache. Underlying error: {exc}"
        ) from exc

    _auth_built = True
    log.info("spnego.auth_initialized", ccache="default")
    return _auth


def reset_cache() -> None:
    """Clear the cached auth object (for tests / reconfiguration)."""
    global _auth, _auth_built
    _auth = None
    _auth_built = False