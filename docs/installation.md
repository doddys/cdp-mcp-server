# Installation

## Requirements

- Python 3.11+
- Access to a Cloudera Manager instance

## Install

```bash
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server
uv venv --python 3.12
uv sync --extra dev        # installs from uv.lock — pins a known-good mcp 1.x
```

> Use `uv sync` rather than a lockless `uv pip install -e .` / `pip install -e .`:
> those resolve fresh and can pull an incompatible major `mcp` (2.0 removed the
> `mcp.server.fastmcp` import the server uses). `uv.lock` pins the working 1.x.
> Plain `pip install -e ".[dev]"` is also safe now that `pyproject.toml` caps
> `mcp<2`, and is what CI uses.

## Registry Backends

### FileRegistry (recommended for teams)

Create `cm_instances.yaml` from the example:

```bash
cp cm_instances.yaml.example cm_instances.yaml
# Edit with your CM credentials
```

```yaml
instances:
  - host: cm.example.com
    port: 7183
    username: admin
    password: "${CM_PASSWORD}"
    environment_name: dev
    use_tls: true
    verify_ssl: false
    api_version: v51   # adjust to your CM version (v40–v54)
```

Run:
```bash
REGISTRY_BACKEND=file cdp-mcp
```

### EnvRegistry (single CM, zero config)

```bash
REGISTRY_BACKEND=env \
  CM_HOST=cm.example.com \
  CM_PORT=7183 \
  CM_USERNAME=admin \
  CM_PASSWORD=changeme \
  CM_USE_TLS=true \
  CM_API_VERSION=v51 \
  cdp-mcp
```

### IcebergRegistry (CDP production)

Original dvergari mode — requires Impala/HiveServer2 with Iceberg table.
See `.env.example` for `IMPALA_*` variables.

## Claude Desktop Configuration

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "env",
        "CM_HOST": "cm.example.com",
        "CM_PORT": "7183",
        "CM_USERNAME": "admin",
        "CM_PASSWORD": "changeme",
        "CM_USE_TLS": "true",
        "CM_API_VERSION": "v51"
      }
    }
  }
}
```

Restart Claude Desktop after saving.

## CM API Version

Different CM versions support different API versions:

| CDH/CDP Version | Max API Version |
|---|---|
| CDH 6.x | v31 |
| CDH 7.0-7.1 | v40-v41 |
| CDP 7.1.7+ | v51+ |

Check your CM version at `https://cm-host:7183/api/version` (TLS, default port) or
`http://cm-host:7180/api/version` (no TLS).
