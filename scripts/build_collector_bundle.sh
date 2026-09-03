#!/usr/bin/env bash
set -euo pipefail
#
# build_collector_bundle.sh — build an offline-installable tarball of
# cdp_mcp.collector for deployment at a client site with no internet/PyPI
# access. Run this on a machine WITH internet (this repo's dev machine); the
# *output* tarball is what gets carried into the client's air-gapped
# network, e.g. via the client's approved egress channel in reverse.
#
# The collector (src/cdp_mcp/collector/) only ever imports
# cm_pool/clients/cm_client/registry/config -- never server.py -- so it never
# touches the `mcp` (FastMCP) package at runtime. cdp-mcp's pyproject.toml
# still *declares* mcp, impyla, thrift, thrift-sasl, and pure-sasl as
# required dependencies of the whole package (impyla/thrift* back the
# IcebergRegistry backend, not needed for a typical client-site FileRegistry/
# EnvRegistry deployment) -- a plain `pip install` of the wheel would pull
# all of that in regardless of whether collect.py ever imports it. This
# script instead installs the wheel with --no-deps and vendors only the
# runtime deps the collector subpackage actually imports, so the offline
# bundle stays small and never needs impyla/thrift-sasl/pure-sasl's
# platform-matched compiled wheels or a system libsasl2.
#
# Cross-platform note: the client site's OS/arch is very likely different
# from this build machine's (e.g. building on macOS, deploying to a Linux
# server). --python-platform below tells `uv pip install` to download wheels
# for that target platform instead of the build machine's -- override
# TARGET_PLATFORM if the client site isn't x86_64 Linux (e.g.
# "aarch64-manylinux2014" for arm64 Linux). This works cleanly for every
# vendored dependency EXCEPT the optional Kerberos extra (WITH_KERBEROS=true
# below) -- see the guard around its install for why.
#
# Usage: scripts/build_collector_bundle.sh [output_dir]
# Produces: <output_dir>/cdp-collector-<version>-<platform>.tar.gz
# Env vars: TARGET_PLATFORM, TARGET_PYTHON, WITH_KERBEROS (default false)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/dist/collector-bundle}"
TARGET_PLATFORM="${TARGET_PLATFORM:-x86_64-manylinux2014}"
TARGET_PYTHON="${TARGET_PYTHON:-3.12}"
WITH_KERBEROS="${WITH_KERBEROS:-false}"

VERSION="$(cd "$REPO_ROOT" && python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml","rb"))["project"]["version"])')"
BUNDLE_NAME="cdp-collector-${VERSION}-${TARGET_PLATFORM}"

BUILD_DIR="$(mktemp -d)"
BUNDLE_DIR="$BUILD_DIR/$BUNDLE_NAME"
VENDOR_DIR="$BUNDLE_DIR/vendor"
mkdir -p "$VENDOR_DIR"

echo "==> Building cdp-mcp wheel"
cd "$REPO_ROOT"
rm -f dist/*.whl
# The wheel itself is pure Python (py3-none-any) -- --python here only picks
# which interpreter runs the build backend, unrelated to TARGET_PLATFORM/
# TARGET_PYTHON below. Pinned to the repo's own .venv to avoid uv resolving
# some other unrelated interpreter version that may not be installed.
uv build --wheel --python "$REPO_ROOT/.venv/bin/python"
WHEEL="$(ls -t dist/*.whl | head -1)"

echo "==> Vendoring the wheel itself (--no-deps: its own code only)"
uv pip install "$WHEEL" --no-deps \
    --target "$VENDOR_DIR" \
    --python-platform "$TARGET_PLATFORM" \
    --python-version "$TARGET_PYTHON"

# Curated runtime deps the collector subpackage actually imports
# (cm_pool.py, clients/*.py, cm_client.py, registry/{base,file_registry,
# env_registry}.py, config.py) -- deliberately excludes mcp (server.py only)
# and impyla/thrift/thrift-sasl/pure-sasl (registry/iceberg.py's _connect()
# only, never reached by collect.py against a file/env registry backend).
echo "==> Vendoring collector runtime dependencies"
uv pip install \
    "httpx[socks]>=0.27.0" \
    "tenacity>=8.3.0" \
    "pydantic>=2.7.0" \
    "pydantic-settings>=2.3.0" \
    "structlog>=24.2.0" \
    "python-dotenv>=1.0.0" \
    "python-dateutil>=2.9.0" \
    "pyyaml>=6.0.0" \
    --target "$VENDOR_DIR" \
    --python-platform "$TARGET_PLATFORM" \
    --python-version "$TARGET_PYTHON"

if [ "$WITH_KERBEROS" = "true" ]; then
    # gssapi (httpx-gssapi's dependency) has NO prebuilt wheel on PyPI for
    # any platform -- it's a C extension compiled against system MIT krb5
    # headers. --python-platform above only steers wheel *selection*; when
    # no wheel matches, `uv pip install` silently falls back to compiling
    # from source with the BUILD machine's native toolchain, ignoring
    # --python-platform entirely. Confirmed live: building this extra for
    # x86_64-manylinux2014 from a macOS arm64 host produced Mach-O arm64
    # .so files vendored under a directory labeled for Linux x86_64 -- an
    # import that would fail at the client site with no build-time warning.
    # So this only proceeds when host and target actually match; otherwise
    # it stops and tells you to build inside a matching Linux container.
    HOST_OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    HOST_ARCH="$(uname -m)"
    [ "$HOST_ARCH" = "arm64" ] && HOST_ARCH="aarch64"
    case "$TARGET_PLATFORM" in
        *"$HOST_ARCH"*) ARCH_MATCH=true ;;
        *) ARCH_MATCH=false ;;
    esac
    if [ "$HOST_OS" != "linux" ] || [ "$ARCH_MATCH" != "true" ]; then
        cat >&2 <<EOF

ERROR: WITH_KERBEROS=true requires building on a host matching
TARGET_PLATFORM ($TARGET_PLATFORM) -- this host is $HOST_OS/$HOST_ARCH.
gssapi has no prebuilt wheel to fall back to; building it here would
silently vendor a binary for the wrong platform (see comment above).

Build inside a matching Linux container instead, e.g. from this repo root:

  docker run --rm --platform linux/amd64 \\
      -v "$REPO_ROOT":/repo -w /repo \\
      -e TARGET_PLATFORM=$TARGET_PLATFORM -e TARGET_PYTHON=$TARGET_PYTHON \\
      -e WITH_KERBEROS=true \\
      python:${TARGET_PYTHON}-slim bash -c '
          apt-get update && \\
          apt-get install -y --no-install-recommends libkrb5-dev gcc pkg-config curl ca-certificates && \\
          curl -LsSf https://astral.sh/uv/install.sh | sh && \\
          export PATH="\$HOME/.local/bin:\$PATH" && \\
          scripts/build_collector_bundle.sh
      '

(--platform linux/amd64 matters on Apple Silicon Docker Desktop, which
defaults to arm64 containers; drop it, or swap in linux/arm64, for an
aarch64 target.)
EOF
        exit 1
    fi
    echo "==> Vendoring Kerberos/SPNEGO support (httpx-gssapi) -- native build on matching platform"
    uv pip install "httpx-gssapi>=0.6.0" --target "$VENDOR_DIR" \
        --python-platform "$TARGET_PLATFORM" --python-version "$TARGET_PYTHON"
fi

cp "$REPO_ROOT/cm_instances.yaml.example" "$BUNDLE_DIR/"

cat > "$BUNDLE_DIR/run_collect.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# run_collect.sh — entry point for the offline collector bundle. Requires
# only a system Python matching TARGET_PYTHON at build time; no internet or
# pip/PyPI access needed -- all dependencies are already vendored below.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$HERE/vendor:${PYTHONPATH:-}"
export REGISTRY_BACKEND="${REGISTRY_BACKEND:-file}"
export REGISTRY_FILE_PATH="${REGISTRY_FILE_PATH:-$HERE/cm_instances.yaml}"
exec python3 -m cdp_mcp.collector.collect "$@"
EOF
chmod +x "$BUNDLE_DIR/run_collect.sh"

cat > "$BUNDLE_DIR/README.md" <<'EOF'
# cdp-collector offline bundle

Standalone data collector for a CDP cluster whose network an LLM client
cannot reach. No LLM, no MCP transport -- calls Cloudera Manager's REST API
directly and writes full-resolution JSON straight to local files. Requires
only a system Python interpreter matching this bundle's target version; no
internet or pip/PyPI access needed at the client site.

## Setup
1. Copy `cm_instances.yaml.example` to `cm_instances.yaml` and fill in this
   site's CM credentials. Never commit this file or carry it back out.
2. `./run_collect.sh --list-clusters` to confirm connectivity and see the
   cluster names known to this CM instance.

## Collect
    ./run_collect.sh --cluster <name> \
        --period-start 2026-08-01T00:00:00Z \
        --period-end   2026-09-01T00:00:00Z \
        --out output/<name>_<period>/

Optional: `--services svc1,svc2` to restrict which services get metrics
collected (default: all services CM reports for the cluster); `--concurrency
N` to change how many hosts are polled for metrics at once (default 4).

Re-running with the same `--out` directory resumes: any entity already
recorded in `output/<name>_<period>/manifest.json` (by exact relative path)
is skipped rather than re-fetched, so an interrupted run or a network blip
partway through a large cluster doesn't mean starting over.

## After collection
`output/<name>_<period>/` is self-contained: `manifest.json` (sha256 +
counts for every file, so it can be verified after transfer) plus a flat set
of `NN_<name>.json` files (`01_host_status.json`, `02_roles_hdfs.json`,
`03_service_metrics_yarn.json`, `06_alerts_CRITICAL_w2.json`, ...) -- the
same filenames and per-entity content cdp-report's export skill produces
when run interactively against a network-reachable cluster, so its Phase
1.5+ (curate/render) tooling can run against this directory directly. Move
the whole directory out through whatever egress channel is approved for
this site (SFTP, encrypted USB, the client's file-transfer portal -- this
bundle has no opinion on that), then run cdp-report's curate/render phases
against it on a separate,
internet-connected machine.

Nothing in this bundle masks hostnames/IPs before they leave the network --
treat the output directory as sensitive and control who/what can access it
in transit.
EOF

echo "==> Packaging tarball"
mkdir -p "$OUT_DIR"
tar -C "$BUILD_DIR" -czf "$OUT_DIR/$BUNDLE_NAME.tar.gz" "$BUNDLE_NAME"
rm -rf "$BUILD_DIR"

CHECKSUM_CMD="sha256sum"
command -v sha256sum >/dev/null 2>&1 || CHECKSUM_CMD="shasum -a 256"

echo "==> Built $OUT_DIR/$BUNDLE_NAME.tar.gz"
$CHECKSUM_CMD "$OUT_DIR/$BUNDLE_NAME.tar.gz"
