"""
errors.py — Cross-cutting exceptions shared across downstream service clients.

SPNEGO detection is an HTTP-protocol-level concern (401 + WWW-Authenticate:
Negotiate), not domain-specific to YARN vs. HDFS vs. Spark vs. Oozie, so it
gets one shared type here instead of four near-identical per-client classes.
"""
from __future__ import annotations


class SpnegoRequiredError(Exception):
    """Raised when a downstream endpoint challenges with SPNEGO (401 + WWW-Authenticate: Negotiate)."""


class SpnegoConfigError(Exception):
    """Raised when Kerberos/SPNEGO is enabled (kerberos=true) but the optional
    `httpx-gssapi` dependency is not installed or credentials are unavailable.

    Surfaces a single actionable message toward server.py instead of an
    ImportError/traceback, per architectural rule #2 (no untyped exceptions)."""
