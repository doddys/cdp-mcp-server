"""
spnego.py — Lazy factory for the SPNEGO auth object attached to the four
downstream service clients (NameNode/YARN/Spark/Oozie) on Kerberized clusters.

Design notes
------------
- `httpx-gssapi` (and its native `gssapi` build dependency) is an OPTIONAL
  install: non-Kerberized users never need krb5 libs. So the import is lazy —
  it only happens when a CM instance has `kerberos=true`. The package is
  declared as a `[kerberos]` extra in pyproject.toml.
- Two credential sources, picked by the caller:
    * Default credentials cache (the original path): no `keytab`/`principal`
      passed. `HTTPSPNEGOAuth()` reads whatever `kinit` (or a keytab loader)
      put in the default ccache. For a long-running server the operator must
      keep that ccache valid (cron `kinit -R` / a keytab-renewed TGT).
    * In-process keytab acquisition (unattended production): `keytab` +
      `principal` passed. We acquire a TGT directly from the keytab via
      `gssapi.Credentials(usage='initiate', name=..., store={'client_keytab': ...})`
      and hand it to `HTTPSPNEGOAuth(creds=...)`. gssapi re-reads the keytab,
      so no external `kinit`/renewer is required.
- Renewal: the downstream client factories in cm_pool.py build a fresh client
  (and therefore call this factory) per tool invocation. In keytab mode we
  acquire fresh credentials on each call rather than caching a snapshot — so
  an expired TGT is transparently re-acquired from the keytab. The default
  ccache path keeps its process-wide cache (the ccache is shared and gssapi
  re-reads it per handshake, so a renewed TGT is picked up without a rebuild).
- Only the downstream clients get this auth. cm_client.py always uses Basic
  auth and is never modified here.
- Never throws an untyped exception toward server.py (architectural rule #2):
  import/credential failures raise the typed `SpnegoConfigError`.
"""
from __future__ import annotations

import os
from typing import Any

import structlog

from cdp_mcp.clients.errors import SpnegoConfigError

log = structlog.get_logger(__name__)

# Cached auth object per process for the *default ccache* path only.
# HTTPSPNEGOAuth is stateless w.r.t. the ccache and safe to share across
# requests/clients; gssapi reads the ccache per handshake, so a renewed TGT is
# picked up without rebuilding this. The keytab path deliberately does NOT use
# this cache (see "Renewal" above).
_auth: Any | None = None
_auth_built = False


def build_spnego_auth(
    kerberos: bool,
    keytab: str | None = None,
    principal: str | None = None,
) -> Any:
    """Return an httpx Auth object for SPNEGO, or None when Kerberos is off.

    Args:
        kerberos: the CM instance's `kerberos` flag.
        keytab: optional path to a keytab. When set, credentials are acquired
            in-process from the keytab (gssapi.Credentials with a cred-store
            `keytab` entry) instead of the default ccache. Requires `principal`.
        principal: the Kerberos principal to acquire (e.g. ``mcp-svc@REALM``).
            Required when `keytab` is set; ignored otherwise.

    Returns:
        An ``httpx_gssapi.HTTPSPNEGOAuth`` instance when kerberos=True, else None.

    Raises:
        SpnegoConfigError: if kerberos=True but httpx-gssapi is not installed,
            the keytab config is incomplete/invalid, or no usable credentials
            could be acquired.
    """
    if not kerberos:
        return None

    # ── In-process keytab acquisition ──────────────────────────────────────
    if keytab:
        return _build_keytab_auth(keytab, principal)

    # ── Default credentials cache (original path) ──────────────────────────
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
    log.info("spnego.auth_initialized", source="ccache")
    return _auth


def _build_keytab_auth(keytab: str, principal: str | None) -> Any:
    """Build an HTTPSPNEGOAuth backed by in-process keytab acquisition.

    Acquires a fresh TGT from the keytab on every call (no process cache) so an
    expired ticket is transparently re-acquired — the caller (cm_pool) builds a
    new downstream client per tool invocation, so this is one KDC AS-REQ per
    Kerberized tool call, which is acceptable for a read-only troubleshooting
    server.
    """
    if not principal:
        raise SpnegoConfigError(
            "Kerberos/SPNEGO keytab mode is enabled (kerberos_keytab is set) "
            "but no principal was configured. Set kerberos_principal "
            "(e.g. 'mcp-svc@REALM') alongside kerberos_keytab."
        )

    if not os.path.isfile(keytab):
        raise SpnegoConfigError(
            f"Kerberos/SPNEGO keytab not found at {keytab!r}. Check the "
            "kerberos_keytab path and that the server user can read it "
            "(chmod 600, owned by the service user)."
        )

    try:
        import gssapi
        from httpx_gssapi import HTTPSPNEGOAuth
    except ImportError as exc:
        raise SpnegoConfigError(
            "Kerberos/SPNEGO is enabled (kerberos=true) but the optional "
            "`httpx-gssapi`/`gssapi` package is not installed. Install it with "
            "the [kerberos] extra (e.g. `uv pip install -e '.[kerberos]'`) and "
            "ensure MIT krb5 is available. On macOS build against brew krb5: "
            "PKG_CONFIG_PATH=/opt/homebrew/opt/krb5/lib/pkgconfig."
        ) from exc

    try:
        name = gssapi.Name(principal, gssapi.NameType.kerberos_principal)
        # cred_store extension: acquire a TGT in-process from the keytab.
        # IMPORTANT: the store key for INITIATING (client TGT, kinit-equivalent
        # AS-REQ) is 'client_keytab' — NOT 'keytab', which is the acceptor/
        # service keytab and does NOT acquire a TGT. The TGT is stored in a
        # process-local MEMORY ccache (no default-ccache pollution, no
        # KRB5CCNAME). usage='initiate' => client credentials.
        creds = gssapi.Credentials(
            usage="initiate",
            name=name,
            store={"client_keytab": keytab, "ccache": "MEMORY:cdp-mcp"},
        )
        auth = HTTPSPNEGOAuth(creds=creds)
    except Exception as exc:
        # gssapi raises MissingCredentialsError / ExpiredCredentialsError etc.
        raise SpnegoConfigError(
            "Kerberos/SPNEGO keytab acquisition failed. Verify the keytab "
            f"contains principal {principal!r}, the realm/KDC in krb5.conf is "
            f"correct, and the KDC is reachable (gssapi does the AS-REQ over "
            f"krb5, not ALL_PROXY — the KDC must be directly reachable or via a "
            f"TCP forward such as localhost:1088). Underlying error: {exc}"
        ) from exc

    log.info(
        "spnego.auth_initialized",
        source="keytab",
        principal=principal,
        keytab=keytab,
    )
    return auth


def reset_cache() -> None:
    """Clear the cached auth object (for tests / reconfiguration)."""
    global _auth, _auth_built
    _auth = None
    _auth_built = False