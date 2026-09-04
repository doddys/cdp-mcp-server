# cdp-mcp-server

MCP server for **Cloudera Manager / CDP** cluster administration and troubleshooting via AI assistants (Claude, etc.).

Fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — extended with pluggable registry backends and additional service clients.

## Features

- **Pluggable registry**: FileRegistry (YAML), EnvRegistry (env vars), IcebergRegistry (Impala/Iceberg)
- **Auto-discovery**: YARN, Spark History Server, HDFS NameNode, Oozie endpoints discovered from CM at startup
- **45 MCP tools**: cluster management, service/role lifecycle, log extraction, metrics, replication, YARN/Spark/HDFS/Oozie diagnostics (full list in [tools.md](tools.md))
- **No Iceberg required**: use FileRegistry or EnvRegistry for quick setup

## Quick Start

```bash
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server
uv venv --python 3.12
uv sync --extra dev
REGISTRY_BACKEND=env CM_HOST=your-cm CM_USERNAME=admin CM_PASSWORD=pass CM_USE_TLS=true CM_API_VERSION=v51 cdp-mcp
```

See [Installation](installation.md) for detailed setup instructions.
For a VPS deployment against a **non-Kerberized** cluster, see
[VPS install (no Kerberos)](vps-install-no-kerberos.md).
For an unattended VPS deployment with autossh + keytab-backed SPNEGO, see
[VPS install (autossh/SPNEGO)](vps-install.md); the underlying SOCKS+kinit
concepts are in [Kerberos tunneling](kerberos-tunneling.md).
To collect data from a cluster your AI assistant **cannot reach** (air-gapped
or restricted networks), see
[Offline collector (cdp-collect)](collector-deploy.md).
