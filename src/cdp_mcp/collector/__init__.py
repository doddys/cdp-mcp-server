"""
cdp_mcp.collector — standalone, offline data collector for CDP clusters that
cannot be reached over the network by an LLM client (air-gapped sites).

No LLM, no FastMCP/MCP transport involved: this subpackage imports
cm_pool/clients/cm_client/registry directly, the same way server.py's tool
functions do, just without the MCP server wrapped around them. It is meant to
be run by hand or via cron inside the client's network; its output directory
(see manifest.py) is the thing that crosses the air-gap boundary afterwards,
carried out through whatever egress channel the client approves.

See scripts/build_collector_bundle.sh for packaging this subpackage plus its
dependencies as an offline-installable tarball for deployment at a client
site that has no cdp-mcp-server checkout and no PyPI access.
"""
