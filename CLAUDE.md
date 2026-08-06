# CLAUDE.md — cdp-mcp-server

MCP server Python per amministrazione e troubleshooting cluster Cloudera/CDP.
Fork di [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — Apache 2.0.

---

## Direttive generali

### Python e tooling
- Usa **poetry** per la gestione del virtualenv e delle dipendenze (NON uv, NON pip diretto).
- Il virtualenv è in `.venv/` (in-project). Creato con `python3.12 -m venv .venv` e poi `poetry env use .venv/bin/python`.
- Per installare: `.venv/bin/pip install -e ".[dev]"` (il pyproject.toml usa poetry-core come build backend).
- Per eseguire comandi nel venv: `.venv/bin/python`, `.venv/bin/pytest`, `.venv/bin/ruff`, ecc.

### Git e GitHub
- Repo personale pubblico su **github.com** (NON github.dxc.com che è aziendale).
- Usa sempre `GH_HOST=github.com gh ...` per tutti i comandi `gh` in questo progetto.
- Non committare mai credenziali o file con dati reali.
- File di configurazione con credenziali → creare sempre `*.example` su git, il file reale nel `.gitignore`.
- Non aggiungere riferimenti a ambienti o host reali nel repository.

### Sessioni di lavoro
- Salva sessioni in `./.claude/sessions/` (path locale del progetto, NON in ~/.claude).
- Usa la skill `save-session` a fine sessione e `load-session` all'inizio.

### Licenza
Apache License 2.0 — stessa del repo originale di dvergari.

---

## Setup rapido

```bash
# Clone
git clone https://github.com/disoardi/cdp-mcp-server.git
cd cdp-mcp-server

# Virtualenv Python 3.12
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Config
cp .env.example .env
cp cm_instances.yaml.example cm_instances.yaml
# Modifica cm_instances.yaml con le credenziali del cluster di test

# Avvio (FileRegistry)
REGISTRY_BACKEND=file cdp-mcp

# Avvio rapido (EnvRegistry — singolo CM)
REGISTRY_BACKEND=env CM_HOST=cm.example.com CM_USERNAME=admin CM_PASSWORD=changeme CM_USE_TLS=false cdp-mcp
```

---

## Struttura del codice

```
src/cdp_mcp/
├── server.py            ← entry point FastMCP, @mcp.tool() definitions
├── config.py            ← Pydantic settings + build_registry() factory
├── cm_client.py         ← httpx async client per CM API (NON toccare senza motivo)
├── cm_pool.py           ← connection pool multi-CM + auto-discovery endpoint
├── registry/
│   ├── base.py          ← BaseRegistry ABC: start/stop/get_all/register/deactivate
│   ├── iceberg.py       ← IcebergRegistry (codice originale dvergari)
│   ├── file_registry.py ← FileRegistry (YAML con interpolazione env)
│   └── env_registry.py  ← EnvRegistry (singolo CM da CM_HOST/CM_PORT/ecc.)
└── clients/
    ├── yarn_client.py   ← YARN ResourceManager REST API (:8088)
    ├── spark_client.py  ← Spark History Server REST API (:18088)
    ├── hdfs_client.py   ← HDFS NameNode JMX (:9870)
    └── oozie_client.py  ← Oozie REST API (:11000)

tests/
├── unit/                ← Test unitari (mock httpx con respx, no dipendenze esterne)
└── integration/         ← Test di integrazione (WireMock via docker-compose)
    ├── docker-compose.yml
    ├── wiremock/        ← Stub definitions per CM, YARN, HDFS, Spark
    └── cm_instances.yaml  ← Gitignored — configurazione locale per i test
```

---

## Regole architetturali

### 1. Non toccare cm_client.py senza motivo
`cm_client.py` è il codice originale di dvergari, testato e funzionante. Modifiche solo per bug fix o estensioni strettamente necessarie.

### 2. I nuovi client seguono lo stesso pattern di cm_client.py
Ogni client in `clients/` deve:
- Usare `httpx.AsyncClient` con timeout esplicito
- Avere retry via `tenacity` per `TransportError` e 503/504
- Avere la stessa gerarchia di eccezioni: `XxxClientError → XxxAuthError → XxxNotFoundError → XxxServiceUnavailable`
- Loggare con `structlog`
- Non lanciare mai eccezioni non tipizzate verso `server.py`

### 3. Auto-discovery, non configurazione manuale
Gli endpoint YARN/Spark/Oozie/HDFS vengono scoperti da CM all'avvio in `cm_pool.py → _discover_service_endpoints()`. Non aggiungere env var per questi URL. Override accettabile: `endpoints_override` in `cm_instances.yaml`.

### 4. I tool in server.py sono read-mostly
I nuovi tool troubleshooting (YARN, Spark, HDFS, Oozie) sono tutti **read-only**. Tool di modifica richiedono discussione esplicita.

### 5. Fallback puliti sui tool applicativi
Se un endpoint non è scoperto → messaggio JSON strutturato, mai traceback.

### 6. Registry backend
| Backend | Uso | Richiede |
|---------|-----|---------|
| `file` | sviluppo, team piccoli | `cm_instances.yaml` |
| `env` | singolo CM, smoke test | env var `CM_HOST`, `CM_USERNAME`, `CM_PASSWORD` |
| `iceberg` | CDP con Impala/Iceberg | Impala/HiveServer2 + tabella Iceberg |

Default: `iceberg` (retrocompatibilità dvergari). Per sviluppo usare `file` o `env`.

---

## Comandi utili

```bash
# Run in sviluppo (stdio)
REGISTRY_BACKEND=file cdp-mcp

# Test unitari (nessuna dipendenza esterna)
.venv/bin/pytest tests/unit/ -v

# Test integrazione (richiede docker compose up)
docker compose -f tests/integration/docker-compose.yml up -d
REGISTRY_BACKEND=file REGISTRY_FILE_PATH=tests/integration/cm_instances.yaml \
  .venv/bin/pytest tests/integration/ -v
docker compose -f tests/integration/docker-compose.yml down

# Smoke test manuale (singolo tool via stdin)
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_clusters","arguments":{}}}' \
  | REGISTRY_BACKEND=env CM_HOST=fake CM_USERNAME=x CM_PASSWORD=x cdp-mcp

# Lint
.venv/bin/ruff check src/
.venv/bin/mypy src/ --ignore-missing-imports
```

---

## Configurazione Claude Desktop

### FileRegistry (consigliato per sviluppo)
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "file",
        "REGISTRY_FILE_PATH": "/path/to/cm_instances.yaml"
      }
    }
  }
}
```

### EnvRegistry (singolo CM, setup minimo)
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "env",
        "CM_HOST": "cm.example.com",
        "CM_USERNAME": "admin",
        "CM_PASSWORD": "changeme",
        "CM_USE_TLS": "false"
      }
    }
  }
}
```

---

## Documentazione GitHub Pages
- La documentazione va in `docs/gh-pages/`.
- Quando è pronta una prima release stabile, genera la documentazione.
- Branch `gh-pages` dedicato per il deploy.

## Kerberos (out of scope MVP)
Per cluster CDP con Kerberos/SPNEGO usare `httpx-gssapi`. Da implementare come opzione `CM_KERBEROS=true`.

### TODO — SPNEGO per i client downstream (YARN/HDFS/Spark/Oozie)
`cm_client.py` usa Basic auth verso CM e funziona. I client in `clients/` (YARN RM,
Spark HS, HDFS NameNode JMX, Oozie) non ricevono alcuna credenziale: su cluster
Kerberizzati le richieste non autenticate tornano una risposta non-JSON (pagina
HTML di negoziazione o corpo vuoto), che oggi si manifesta come errore di parsing
JSON generico.

- Libreria: `httpx-gssapi` (implementa `httpx.Auth`), da agganciare solo ai
  quattro client downstream — non a `cm_client.py`.
- Credenziali da keytab, non da `kinit` interattivo (processo server long-running):
  `KRB5_CLIENT_KTNAME=/path/to/svc.keytab` + principal dedicato, così gssapi
  rinnova automaticamente il ticket.
- Trade-off principale: dipendenza da libreria di sistema (MIT `libkrb5-dev` a
  build time, `krb5-libs` a runtime) → immagine Docker più pesante.
- Punto di innesto: `cm_pool.py` (dove vengono istanziati `YarnClient`,
  `SparkClient`, `HdfsClient`, `OozieClient`) + nuovo flag `CM_KERBEROS=true` in
  `config.py`.
- Non implementare senza discussione esplicita (vedi regola architetturale #4).

## Nota linguaggio futuro
Il PoC è Python + FastMCP. Se validato, valutare riscrittura in Go per distribuzione come binario statico.
