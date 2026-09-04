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
# Produces: <output_dir>/cdp-collector-<version>-<platform>-py<X.Y>[-kerberos].tar.gz
# Env vars: TARGET_PLATFORM, TARGET_PYTHON, WITH_KERBEROS (default false)
#
# Python 3.8-3.10 support: the wheel's metadata declares requires-python
# >=3.11 (the MCP server's floor -- mcp 1.x needs 3.10+), but the COLLECTOR
# subpackage alone is 3.8-compatible (verified: full import chain compiles
# and runs on 3.8 with these exact vendored pins + eval_type_backport for
# pydantic's `X | Y` annotations). Wheels are installed --no-deps below, so
# the metadata gate never applies at the client site; the only place it
# bites is uv's resolver when TARGET_PYTHON < 3.11, which we work around
# by rewriting the wheel's Requires-Python before vendoring.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/dist/collector-bundle}"
TARGET_PLATFORM="${TARGET_PLATFORM:-x86_64-manylinux2014}"
TARGET_PYTHON="${TARGET_PYTHON:-3.12}"
WITH_KERBEROS="${WITH_KERBEROS:-false}"

# tomllib is 3.11+; a TARGET_PYTHON<3.11 build container (see the Docker
# recipe below) runs an older python3, so fall back to a regex there.
VERSION="$(cd "$REPO_ROOT" && python3 -c '
try:
    import tomllib
    print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])
except ModuleNotFoundError:
    import re
    src = open("pyproject.toml").read()
    m = re.search(r"^version\s*=\s*\"([^\"]+)\"", src, re.M)
    print(m.group(1))
')"

# Rewrite a wheel's Requires-Python from >=3.11 to >=3.8 in-place-ish: unzip,
# edit METADATA + the *.dist-info WHEEL-safe copy, rezip to a sibling file.
# Only touches metadata files -- code bytes identical. Used for
# TARGET_PYTHON < 3.11 builds (see comment near its call site).
_relax_requires_python() {
    local in_wheel="$1"
    local workdir
    workdir="$(mktemp -d)"
    python3 - "$in_wheel" "$workdir" <<'PYEOF'
import os, sys, zipfile

in_wheel, workdir = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(in_wheel) as zf:
    zf.extractall(workdir)

changed = False
for root, _dirs, files in os.walk(workdir):
    for name in files:
        if name not in ("METADATA",):
            continue
        path = os.path.join(root, name)
        with open(path) as f:
            lines = f.readlines()
        out = []
        for line in lines:
            if line.startswith("Requires-Python:") and "3.11" in line:
                out.append("Requires-Python: >=3.8\n")
                changed = True
            else:
                out.append(line)
        with open(path, "w") as f:
            f.writelines(out)

if not changed:
    print("WARNING: no Requires-Python >=3.11 found to relax", file=sys.stderr)
    sys.exit(1)

# Renamed wheel must stay a valid wheel filename (uv validates it): swap the
# build tag slot rather than appending a suffix -- "-py38plus" would be an
# invalid non-numeric build tag.
out_wheel = os.path.join(
    os.path.dirname(in_wheel),
    os.path.basename(in_wheel).replace("-py3-none-any.whl", "-1-py3-none-any.whl"),
)
if out_wheel == in_wheel:  # unexpected filename shape -- bail loudly
    print(f"ERROR: unexpected wheel name {in_wheel!r}", file=sys.stderr)
    sys.exit(1)
with zipfile.ZipFile(out_wheel, "w", zipfile.ZIP_DEFLATED) as zf:
    for root, _dirs, files in os.walk(workdir):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, workdir)
            zf.write(full, rel)
print(out_wheel)
PYEOF
}

# -py<X.Y> in the name: a py3.8 bundle (httpx-gssapi 0.3.1, httpx<0.28) and a
# py3.12 bundle (0.6.x, current httpx) differ in more than platform -- keep
# them from being mistaken for each other at the client site, and from
# overwriting each other in dist/ (they previously collided when both were
# built without -kerberos).
BUNDLE_NAME="cdp-collector-${VERSION}-${TARGET_PLATFORM}-py${TARGET_PYTHON}${WITH_KERBEROS:+-kerberos}"

BUILD_DIR="$(mktemp -d)"
BUNDLE_DIR="$BUILD_DIR/$BUNDLE_NAME"
VENDOR_DIR="$BUNDLE_DIR/vendor"
mkdir -p "$VENDOR_DIR"

cd "$REPO_ROOT"

# A fresh checkout (e.g. one just built on a build box for the first time,
# as opposed to an existing dev machine that already ran `uv sync`) has no
# .venv yet -- bootstrap one rather than failing with uv's opaque
# "No interpreter found at path .venv/bin/python" (hit this literally
# building on a fresh VPS checkout). Version matches the Quick Setup section
# of CLAUDE.md; only the interpreter is needed here, not the project's own
# runtime deps, since this .venv exists solely to run the build backend.
# --clear: a .venv left by a PREVIOUS containerized build in this same
# mounted repo (e.g. a py3.8 run followed by py3.12, or vice versa) is
# foreign -- uv venv refuses to replace a non-empty venv, and a
# mismatched-interpreter leftover breaks uv build below. Always recreate;
# on a dev machine this throws away a working `uv sync` venv, but the
# build only needs the interpreter, not installed deps.
if ! "$REPO_ROOT/.venv/bin/python" -c 'import sys; assert sys.version_info[:2] == (3, 12)' 2>/dev/null; then
    echo "==> Bootstrapping .venv (uv venv --python 3.12 --clear)"
    uv venv --python 3.12 --clear
fi

echo "==> Building cdp-mcp wheel"
rm -f dist/*.whl
# The wheel itself is pure Python (py3-none-any) -- --python here only picks
# which interpreter runs the build backend, unrelated to TARGET_PLATFORM/
# TARGET_PYTHON below. Pinned to the repo's own .venv to avoid uv resolving
# some other unrelated interpreter version that may not be installed.
uv build --wheel --python "$REPO_ROOT/.venv/bin/python"
WHEEL="$(ls -t dist/*.whl | head -1)"

# TARGET_PYTHON < 3.11: rewrite the wheel's Requires-Python so uv's resolver
# (which DOES check the metadata even with --no-deps) accepts it for the
# older target. Safe because the vendored code never executes on the build
# machine and the collector subpackage is genuinely 3.8-compatible; only
# server.py needs 3.11+, and it is excluded from the collector's imports.
case "$TARGET_PYTHON" in
    3.8|3.9|3.10)
        echo "==> TARGET_PYTHON=$TARGET_PYTHON < 3.11: rewriting wheel Requires-Python"
        WHEEL="$(_relax_requires_python "$WHEEL")"
        ;;
esac

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
# eval_type_backport: only needed for TARGET_PYTHON < 3.11 -- lets pydantic
# evaluate the `X | Y` annotations config.py's models use on old interpreters.
# Dropped from the pin list on 3.11+ via the conditional below.
echo "==> Vendoring collector runtime dependencies"
DEP_PINS=(
    "httpx[socks]>=0.27.0"
    "tenacity>=8.3.0"
    "pydantic>=2.7.0"
    "pydantic-settings>=2.3.0"
    "structlog>=24.2.0"
    "python-dotenv>=1.0.0"
    "python-dateutil>=2.9.0"
    "pyyaml>=6.0.0"
)
case "$TARGET_PYTHON" in
    3.8)
        # httpx-gssapi 0.3.1 (the last 3.8-capable release, 0.4 dropped 3.8)
        # pins httpx<0.28 -- cap httpx here too so the resolver picks 0.27.x
        # ONCE for both this step and the Kerberos step below; resolving 0.28
        # here and 0.27 there would vendor two httpx versions layered in the
        # same --target dir (uv overwrites, but only after warning).
        DEP_PINS=("httpx[socks]>=0.27.0,<0.28" "${DEP_PINS[@]:1}")
        DEP_PINS+=("eval_type_backport>=0.2")
        ;;
    3.9|3.10) DEP_PINS+=("eval_type_backport>=0.2") ;;
esac
uv pip install "${DEP_PINS[@]}" \
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
    # httpx-gssapi 0.4 dropped Python 3.7/3.8; 0.3.1 is the last 3.8-capable
    # release (pins httpx<0.28, matched above in DEP_PINS). Both support the
    # HTTPSPNEGOAuth(creds=...) keytab path spnego.py uses.
    GSSAPI_PIN="httpx-gssapi>=0.6.0"
    [ "$TARGET_PYTHON" = "3.8" ] && GSSAPI_PIN="httpx-gssapi==0.3.1"
    # gssapi compiles from source (no wheels); the compiler targets whatever
    # interpreter uv resolves. Inside the Docker recipe the SYSTEM python3 IS
    # TARGET_PYTHON, so pin --python to it explicitly for a matching ABI tag
    # (cp38) -- otherwise uv prefers the bootstrapped .venv's 3.12 and the
    # built gssapi wheel gets tagged cp312 and rejected below, exactly as if
    # this had been built on the wrong platform.
    UV_PYTHON_ARGS=(--python "$REPO_ROOT/.venv/bin/python")
    if command -v python3 >/dev/null && \
       TARGET_PYTHON="$TARGET_PYTHON" python3 -c 'import os, sys; sys.exit(0 if "%d.%d" % sys.version_info[:2] == os.environ["TARGET_PYTHON"] else 1)'; then
        UV_PYTHON_ARGS=(--python "$(command -v python3)")
    fi
    uv pip install "$GSSAPI_PIN" --target "$VENDOR_DIR" \
        "${UV_PYTHON_ARGS[@]}" \
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
recorded in `output/<name>_<period>/_manifest.json` (by exact relative path)
is skipped rather than re-fetched, so an interrupted run or a network blip
partway through a large cluster doesn't mean starting over. A file written
as {"status": "not_available"} after a failed call is retried on resume --
fix the cause (permissions, kinit, network) and re-run the same command.

## After collection
`output/<name>_<period>/` is self-contained: `_manifest.json` (sha256 +
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

# 3.8+Kerberos bundles ship httpx-gssapi 0.3.1 (0.4 dropped 3.8), which
# predates 0.5's "use the SPNEGO mechanism by default" change. Against a
# standard MIT krb5 KDC the two negotiate identically, but against an
# Active Directory KDC the older default (krb5 mechanism) has historically
# caused interop friction. Append a targeted caveat to the bundle's own
# README (after the quoted heredoc above, so nothing in it can expand) so
# an operator at the client site knows to try the py3.12 bundle first
# (httpx-gssapi 0.6.x, SPNEGO-by-default) if the environment uses AD.
if [ "$WITH_KERBEROS" = "true" ] && [ "$TARGET_PYTHON" = "3.8" ]; then
    cat >> "$BUNDLE_DIR/README.md" <<'CAVEAT'

## Active Directory environments
This 3.8 bundle ships httpx-gssapi 0.3.1 (the last release supporting
Python 3.8; 0.4 dropped it). That version predates httpx-gssapi 0.5's
switch to the SPNEGO mechanism by default and negotiates with the plain
krb5 mechanism instead. Against a standard MIT krb5 KDC the two behave
identically; against an **Active Directory** KDC the older default has
historically caused mechanism-negotiation failures. If SPNEGO
authentication errors occur here and the site runs AD, prefer the py3.12
bundle (httpx-gssapi 0.6.x, SPNEGO-by-default) on a host with Python 3.12.
CAVEAT
fi


echo "==> Packaging tarball"
mkdir -p "$OUT_DIR"
tar -C "$BUILD_DIR" -czf "$OUT_DIR/$BUNDLE_NAME.tar.gz" "$BUNDLE_NAME"
rm -rf "$BUILD_DIR"

CHECKSUM_CMD="sha256sum"
command -v sha256sum >/dev/null 2>&1 || CHECKSUM_CMD="shasum -a 256"

echo "==> Built $OUT_DIR/$BUNDLE_NAME.tar.gz"
$CHECKSUM_CMD "$OUT_DIR/$BUNDLE_NAME.tar.gz"
