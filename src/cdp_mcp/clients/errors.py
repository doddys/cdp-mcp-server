"""
errors.py — Cross-cutting exceptions shared across downstream service clients.

SPNEGO detection is an HTTP-protocol-level concern (401 + WWW-Authenticate:
Negotiate), not domain-specific to YARN vs. HDFS vs. Spark vs. Oozie, so it
gets one shared type here instead of four near-identical per-client classes.
"""
from __future__ import annotations


class SpnegoRequiredError(Exception):
    """Raised when a downstream endpoint challenges with SPNEGO (401 + WWW-Authenticate: Negotiate)."""
