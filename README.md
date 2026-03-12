# Stock Exchange Data Pipeline

A production-style **Bronze → Silver → Gold** data pipeline that streams real-time stock exchange events from the [DE-Stock](../mock-stock) Rust simulator, processes them with Pandas, stores results as partitioned Parquet files, and exposes analytics-ready aggregates via DuckDB — all orchestrated by Dagster.

---

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Project structure](#project-structure)
4. [Local development — step by step](#local-development--step-by-step)
5. [Docker — full stack](#docker--full-stack)
6. [Configuration reference](#configuration-reference)
7. [Dagster UI guide](#dagster-ui-guide)
8. [Running tests](#running-tests)
9. [Linting and type checking](#linting-and-type-checking)
10. [Data layers explained](#data-layers-explained)
11. [Kafka topics](#kafka-topics)
12. [Querying the data lake](#querying-the-data-lake)
13. [Troubleshooting](#troubleshooting)

---

## Architecture overview

```
┌─────────────────────────────────────────────────────┐
│  DE-Stock Simulator  (Rust, ws://localhost:8080)     │
│  7 symbols · order-book matching · circuit breakers  │
└──────────────────────────┬──────────────────────────┘
                           │  WebSocket JSON events
                           ▼
              dagster_stock/utils/ws_to_kafka.py
              (bridges WS → Kafka, configurable topics)
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   stock.trades     stock.orders     stock.quotes
   stock.halts      stock.stats      stock.orderbook
                    stock.agents
                           │
                           ▼
┌─────────────────── Dagster Pipeline ───────────────────┐
│                                                         │
│  Bronze  (schema-on-read, raw Parquet)                  │
│  ├── bronze_trades        bronze_quotes                 │
│  ├── bronze_orders        bronze_market_stats           │
│  ├── bronze_trading_halts bronze_order_book             │
│  └── bronze_agent_actions                               │
│                   │                                     │
│  Silver  (schema-on-write, cleaned + typed)             │
│  ├── silver_trades   (dedup, cast, derive side)         │
│  ├── silver_quotes   (rename fields, spread_bps, …)     │
│  └── silver_orders   (fill_status, normalise agent)     │
│                   │                                     │
│  Gold  (DuckDB SQL aggregates, analytics-ready)         │
│  ├── gold_ohlcv             (OHLCV + VWAP per day)      │
│  ├── gold_market_quality    (spread, depth, score)      │
│  ├── gold_agent_pnl         (P&L by agent type)         │
│  └── gold_circuit_breaker   (halt analysis)             │
│                                                         │
│  Asset checks · Schedules · Sensors                     │
└─────────────────────────────────────────────────────────┘
                           │
                     Parquet files
              ./data/{bronze,silver,gold}/<table>/
                  year=YYYY/month=MM/day=DD/data.parquet
                   (or s3://stock-lake/… for MinIO)
```

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.11 | Pipeline runtime |
| Rust + Cargo | stable (≥ 1.78) | Build & run mock-stock simulator |
| Docker + Compose | ≥ 24 / ≥ 2.20 | Run Kafka, MinIO, Grafana (optional for local dev) |
| Git | any | Clone repos |

> **macOS**: install Rust via `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
> **Python**: use `pyenv` or the system Python ≥ 3.11

---

## Project structure

```
stock-pipeline/           ← this repo
├── dagster_stock/
│   ├── definitions.py    ← Dagster entrypoint
│   ├── assets/           ← bronze / silver / gold @asset functions
│   ├── checks/           ← @asset_check data quality functions
│   ├── resources/        ← Kafka, DuckDB, Storage resources
│   ├── sensors/          ← kafka_lag_sensor, gold_freshness_sensor
│   ├── schedules/        ← daily/hourly schedules
│   ├── jobs/             ← bronze/silver/gold/full pipeline jobs
│   ├── io_managers/      ← ParquetIOManager
│   └── utils/
│       ├── ws_to_kafka.py  ← WebSocket → Kafka bridge
│       └── schemas.py      ← Pydantic models for mock-stock events
├── tests/
├── monitoring/           ← Prometheus + Grafana configs
├── docker-compose.yml
├── pyproject.toml
├── dagster.yaml          ← local SQLite Dagster config
└── .env.example          ← all env vars documented

../mock-stock/            ← companion repo (Rust simulator)
├── src/
├── config/
│   ├── default.toml      ← original single-symbol config
│   └── pipeline.toml     ← multi-symbol config for this pipeline ✓
└── Dockerfile
```

---

## Local development — step by step

This is the recommended workflow for development. Each component runs in a separate terminal.

### Step 1 — Clone and install

```bash
# If you haven't already
git clone <this-repo> stock-pipeline
cd stock-pipeline

# Install Python package (editable) + dev deps
pip install -e ".[dev]"
```

### Step 2 — Start Kafka and MinIO (Docker)

You only need to spin up the infrastructure containers. The simulator and Dagster run as local processes.

```bash
docker compose up -d kafka minio minio-init
```

Wait for Kafka to be healthy (≈ 15 s):

```bash
docker compose ps          # STATUS should show "healthy" for kafka
# or
docker logs kafka 2>&1 | grep "started (kafka.server.KafkaServer)"
```

MinIO console is available at http://localhost:9001 (user: `minioadmin`, pass: `minioadmin`).
The `stock-lake` bucket is created automatically by `minio-init`.

> **No Docker?** Install Kafka locally and set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`.
> For storage, keep `STORAGE_BACKEND=local` (no MinIO needed).

### Step 3 — Configure environment

```bash
cp .env.example .env
```

For pure local development (no S3/MinIO), the defaults are fine as-is:

```bash
# .env — minimal local override
STORAGE_BACKEND=local
LOCAL_DATA_DIR=./data
WS_URL=ws://localhost:8080
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Load it in every terminal that runs pipeline code:

```bash
export $(grep -v '^#' .env | xargs)
# or use: source .env (if you use a tool like direnv)
```

### Step 4 — Build and start mock-stock (Terminal 1)

```bash
cd ../mock-stock

# First run: builds the Rust binary (~60 s)
cargo build --release

# Start the simulator using the pipeline config (7 symbols, 50ms ticks)
cargo run --release -- config/pipeline.toml
```

Expected output:
```
[INFO] Starting DE-Stock Exchange Simulator
[INFO] Loaded 7 symbols: AAPL, MSFT, NVDA, TSLA, JPM, AMZN, SMCI
[INFO] WebSocket server listening on ws://0.0.0.0:8080
[INFO] Simulation running at 50ms tick interval
```

The simulator streams events continuously until you press `Ctrl+C`.

> **Customise the simulation**: edit `../mock-stock/config/pipeline.toml`
> - Add/remove symbols with `[[symbols]]` sections
> - Change `tick_interval_ms` (lower = faster, more events)
> - Increase agent counts for more trading activity
> - Enable `[data_quality] enabled = true` to inject duplicates/out-of-order events for robustness testing

### Step 5 — Start the WebSocket → Kafka bridge (Terminal 2)

```bash
cd stock-pipeline
python dagster_stock/utils/ws_to_kafka.py
```

Expected output:
```
2026-03-11 10:00:01 INFO Kafka target: localhost:9092
2026-03-11 10:00:01 INFO Topic routing: {'TradeExecuted': 'stock.trades', ...}
2026-03-11 10:00:01 INFO Connecting to ws://localhost:8080 (attempt 1) …
2026-03-11 10:00:01 INFO Simulator: Connected to DE-Stock Exchange Simulator
2026-03-11 10:00:01 INFO Connected. Routing events to Kafka …
2026-03-11 10:00:08 INFO Published 500 events (0 skipped) — last: TradeExecuted → stock.trades
```

The bridge auto-reconnects if the simulator restarts. Press `Ctrl+C` to stop.

> **Topic overrides**: set `TOPIC_TRADES=my.custom.topic` etc. before starting.

### Step 6 — Start Dagster (Terminal 3)

```bash
cd stock-pipeline
dagster dev -f dagster_stock/definitions.py
```

Expected output:
```
2026-03-11 10:00:05 - dagster - INFO - Serving dagster-webserver on http://127.0.0.1:3000
```

Open **http://localhost:3000** in your browser.

> The `dagster dev` command starts both the webserver and the daemon (scheduler + sensor runner) in one process. It's equivalent to `dagster-webserver` + `dagster-daemon run` combined.

### Step 7 — Materialise assets

**Option A — Dagster UI**

1. Click **Assets** in the left sidebar.
2. Click **Materialise all** (top-right) to run the full pipeline.
3. Or click an individual asset → **Materialise** to run just that one.

**Option B — CLI**

```bash
# Entire pipeline
dagster asset materialize -f dagster_stock/definitions.py --select "*"

# One layer at a time (recommended order)
dagster asset materialize -f dagster_stock/definitions.py --select "bronze/*"
dagster asset materialize -f dagster_stock/definitions.py --select "silver/*"
dagster asset materialize -f dagster_stock/definitions.py --select "gold/*"

# Single asset
dagster asset materialize -f dagster_stock/definitions.py --select "gold_ohlcv"
```

### Step 8 — Verify output

After materialising, check the local data lake:

```bash
find ./data -name "*.parquet" | sort
```

Example output:
```
./data/bronze/trades/year=2026/month=03/day=11/data.parquet
./data/bronze/quotes/year=2026/month=03/day=11/data.parquet
./data/silver/trades/year=2026/month=03/day=11/data.parquet
./data/gold/ohlcv/year=2026/month=03/day=11/data.parquet
...
```

Query with Python:
```python
import duckdb
conn = duckdb.connect()
df = conn.execute("SELECT * FROM read_parquet('./data/gold/ohlcv/**/*.parquet')").df()
print(df)
```

---

## Docker — full stack

Start every service (simulator + bridge + Kafka + MinIO + Dagster + Prometheus + Grafana) in one command:

```bash
docker compose up -d
```

> **First run**: building the Rust binary inside Docker takes ~3–5 minutes. Subsequent starts use the Docker layer cache and are fast.

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Dagster UI | http://localhost:3010 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:9092 | — |
| mock-stock WS | ws://localhost:8080 | — |

### Useful Docker commands

```bash
# View logs
docker compose logs -f mock-stock       # simulator
docker compose logs -f ws-bridge        # Kafka bridge
docker compose logs -f dagster-webserver

# Restart a single service
docker compose restart ws-bridge

# Stop everything (keeps volumes)
docker compose down

# Stop and delete all data volumes (full reset)
docker compose down -v

# Rebuild after code changes
docker compose build mock-stock
docker compose up -d mock-stock
```

### Changing the simulator config in Docker

Edit `../mock-stock/config/pipeline.toml`, then:

```bash
docker compose restart mock-stock ws-bridge
```

---

## Configuration reference

All configuration is via environment variables. See `.env.example` for the full annotated list.

### Storage backend

**Local (default):**
```bash
STORAGE_BACKEND=local
LOCAL_DATA_DIR=./data
```
Parquet files are written to `./data/{layer}/{table}/year=.../month=.../day=.../data.parquet`.

**S3 / MinIO:**
```bash
STORAGE_BACKEND=s3
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
STORAGE_BUCKET=stock-lake
```

### Kafka topics (all overridable)

```bash
TOPIC_TRADES=stock.trades
TOPIC_ORDERS=stock.orders
TOPIC_QUOTES=stock.quotes
TOPIC_ORDERBOOK=stock.orderbook
TOPIC_STATS=stock.stats
TOPIC_HALTS=stock.halts
TOPIC_AGENTS=stock.agents
```

### Bridge tuning

```bash
RECONNECT_DELAY_S=5          # seconds before retrying after disconnect
MAX_RECONNECT_ATTEMPTS=0     # 0 = retry forever
LOG_INTERVAL_N=500           # print progress every N published events
SKIP_UNKNOWN_EVENTS=false    # set true to silently drop unrecognised event_types
```

---

## Dagster UI guide

### Asset graph
**Assets → Global asset lineage** shows the full Bronze → Silver → Gold dependency graph.

### Running jobs
**Jobs → bronze_job / silver_job / gold_job / full_pipeline_job → Launch run**

### Schedules
| Schedule | Cron | Job |
|---|---|---|
| `daily_bronze_schedule` | `0 0 * * *` (midnight UTC) | `bronze_job` |
| `hourly_silver_schedule` | `0 * * * *` (top of hour) | `silver_job` |

Enable via **Automation → Schedules → toggle ON**.

### Sensors
| Sensor | Interval | Behaviour |
|---|---|---|
| `kafka_lag_sensor` | 60 s | Fires `bronze_job` when any topic lag > 500 messages |
| `gold_freshness_sensor` | 30 min | Logs error when gold assets are > 25 hours old |

Enable via **Automation → Sensors → toggle ON**.

### Asset checks
After materialising silver or gold assets, click an asset → **Checks** tab to see pass/fail status for data quality rules (null checks, price bounds, spread validity, VWAP deviation, etc.).

---

## Running tests

```bash
# All tests
pytest tests/ -v

# Asset tests only
pytest tests/test_assets/ -v

# Quality check tests only
pytest tests/test_checks/ -v

# Filter by name
pytest -k "silver" -v
pytest -k "ohlcv" -v

# With coverage
pytest tests/ --cov=dagster_stock --cov-report=term-missing
```

Tests use `tmp_path` fixtures and mock resources — no running Kafka or Dagster required.

---

## Linting and type checking

```bash
# Lint (fast, checks style + imports)
ruff check dagster_stock/ tests/

# Auto-fix lint issues
ruff check --fix dagster_stock/ tests/

# Format code
ruff format dagster_stock/ tests/

# Type checking
mypy dagster_stock/
```

---

## Data layers explained

### Bronze — raw ingestion
- **What**: stores events exactly as received from Kafka, no transformations.
- **Why**: preserves the original data for replay, debugging, and schema evolution.
- **Pattern**: `kafka.poll(topic) → pd.DataFrame → storage.write_parquet(layer="bronze")`.
- **Special handling**: `bronze_orders` stores both `OrderPlaced` and `OrderCancelled` rows, distinguished by the injected `event_type` column. `bronze_order_book` JSON-serialises the `bids`/`asks` lists.

### Silver — cleaned and typed
- **What**: enforces types, rejects invalids, derives fields, normalises categoricals.
- **Why**: downstream gold assets and checks rely on consistent, typed data.
- **Rejected rows** are written to `./data/rejected/<table>/` for audit.
- Key derivations:

| Asset | Derivation |
|---|---|
| `silver_trades` | `side` from `is_aggressive_buy` (True→"buy") |
| `silver_quotes` | `bid/ask` renamed from `best_bid/ask`; `spread_bps`, `mid_price`, `depth_imbalance` computed |
| `silver_orders` | `fill_status` from event_type; agent_type/side/order_type normalised to lowercase |

### Gold — business aggregates
- **What**: analytical summaries per symbol per day, computed via DuckDB SQL over silver Parquet.
- **Why**: analytics-ready tables for BI, ML features, and reporting.

| Asset | Description |
|---|---|
| `gold_ohlcv` | Open/High/Low/Close/Volume + VWAP from trades; cross-validated against MarketStats |
| `gold_market_quality` | Avg spread bps, depth imbalance, composite quality score per symbol per day |
| `gold_agent_pnl` | Net P&L, volume, trade count per agent type (retail/institutional/hft/market_maker) |
| `gold_circuit_breaker` | Halt count, avg duration, trigger move %, most common reason per symbol per day |

---

## Kafka topics

| Topic | Source events | Consumer |
|---|---|---|
| `stock.trades` | `TradeExecuted` | `bronze_trades` |
| `stock.orders` | `OrderPlaced` + `OrderCancelled` | `bronze_orders` |
| `stock.quotes` | `QuoteUpdate` | `bronze_quotes` |
| `stock.orderbook` | `OrderBookSnapshot` | `bronze_order_book` |
| `stock.stats` | `MarketStats` | `bronze_market_stats` |
| `stock.halts` | `TradingHalt` + `TradingResume` | `bronze_trading_halts` |
| `stock.agents` | `AgentAction` | `bronze_agent_actions` |

All topics are auto-created by Kafka on first publish (no manual setup needed).

Inspect topic lag:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group dagster-stock-pipeline \
  --describe
```

---

## Querying the data lake

### Python + DuckDB
```python
import duckdb

conn = duckdb.connect()

# Gold OHLCV
df = conn.execute("""
    SELECT symbol, trade_date, open, high, low, close, vwap, trade_count
    FROM read_parquet('./data/gold/ohlcv/**/*.parquet')
    ORDER BY trade_date DESC, symbol
""").df()
print(df)

# Agent P&L by type
df = conn.execute("""
    SELECT agent_type, SUM(realized_pnl) AS total_pnl, SUM(trade_count) AS trades
    FROM read_parquet('./data/gold/agent_pnl/**/*.parquet')
    GROUP BY agent_type
    ORDER BY total_pnl DESC
""").df()
print(df)

# Silver trades for AAPL
df = conn.execute("""
    SELECT timestamp, price, quantity, side
    FROM read_parquet('./data/silver/trades/**/*.parquet')
    WHERE symbol = 'AAPL'
    ORDER BY timestamp
    LIMIT 100
""").df()
print(df)
```

### Pandas
```python
import pandas as pd
from pathlib import Path

# Read all gold OHLCV partitions
files = list(Path("./data/gold/ohlcv").rglob("*.parquet"))
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
print(df.groupby("symbol")[["vwap", "volume"]].mean())
```

---

## Troubleshooting

### "No messages received from stock.trades"
- Is mock-stock running? Check Terminal 1 for `WebSocket server listening on ws://0.0.0.0:8080`.
- Is the bridge running? Check Terminal 2 for `Published N events`.
- Is Kafka reachable? `docker compose ps kafka` should show `healthy`.

### Bridge connects but publishes 0 events
- Verify `WS_URL` matches mock-stock's address: `echo $WS_URL`.
- Check bridge logs for `Malformed envelope` or `JSON decode error`.

### Silver assets produce empty DataFrames
- Bronze Parquet must exist first. Run `bronze_job` before `silver_job`.
- Check `event_type` is in bronze DataFrame: `df["event_type"].value_counts()`.

### Gold DuckDB query fails with "no such file"
- Silver Parquet must exist. Materialise silver assets before gold.
- Check the path: `storage.get_parquet_path(layer="silver", table="trades")` should point to an existing directory.

### Dagster UI not loading
- Confirm `dagster dev` is running and shows `Serving dagster-webserver on http://127.0.0.1:3000`.
- In Docker, use port 3010: http://localhost:3010.

### Rust build fails
```bash
rustup update stable          # update Rust toolchain
cargo clean && cargo build --release   # clean build
```

### Port conflicts
| Port | Service | Fix |
|---|---|---|
| 8080 | mock-stock | Change `websocket_port` in `pipeline.toml` and `WS_URL` in `.env` |
| 9092 | Kafka | Change in `docker-compose.yml` and `KAFKA_BOOTSTRAP_SERVERS` |
| 9000 | MinIO | Change in `docker-compose.yml` and `MINIO_ENDPOINT` |
| 3000/3010 | Dagster | Pass `-p <port>` to `dagster dev` |

### Reset everything
```bash
# Delete local data lake
rm -rf ./data

# Reset Docker volumes (Kafka offsets, MinIO data, Dagster state)
docker compose down -v
docker compose up -d kafka minio minio-init
```
