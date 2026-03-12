# CLAUDE.md — Stock Exchange Data Pipeline

This file gives Claude (and any developer) a complete map of the project: what it does, how every piece fits together, what commands to run, and the conventions to follow when making changes.

---

## Project purpose

A **Bronze → Silver → Gold** data pipeline that ingests real-time stock exchange events from a Rust WebSocket simulator, cleans and transforms them with Pandas, and stores analytics-ready aggregates in partitioned Parquet files queryable via DuckDB. Orchestrated by Dagster.

---

## Repository layout

```
stock-pipeline/
├── dagster_stock/
│   ├── __init__.py
│   ├── definitions.py          ← Dagster entrypoint (register everything here)
│   │
│   ├── assets/
│   │   ├── bronze/             ← Layer 1: raw ingestion from Kafka (schema-on-read)
│   │   │   ├── trades.py           bronze_trades         stock.trades
│   │   │   ├── orders.py           bronze_orders         stock.orders
│   │   │   ├── quotes.py           bronze_quotes         stock.quotes
│   │   │   ├── market_stats.py     bronze_market_stats   stock.stats
│   │   │   ├── trading_halts.py    bronze_trading_halts  stock.halts
│   │   │   ├── order_book.py       bronze_order_book     stock.orderbook
│   │   │   └── agent_actions.py    bronze_agent_actions  stock.agents
│   │   │
│   │   ├── silver/             ← Layer 2: cleaned, typed, validated (schema-on-write)
│   │   │   ├── trades.py           silver_trades
│   │   │   ├── quotes.py           silver_quotes
│   │   │   └── orders.py           silver_orders
│   │   │
│   │   └── gold/               ← Layer 3: business aggregates (analytics-ready)
│   │       ├── ohlcv.py            gold_ohlcv
│   │       ├── market_quality.py   gold_market_quality
│   │       ├── agent_pnl.py        gold_agent_pnl
│   │       └── circuit_breaker.py  gold_circuit_breaker
│   │
│   ├── checks/
│   │   ├── silver_checks.py    ← @asset_check: uniqueness, nulls, price bounds, side validity
│   │   └── gold_checks.py      ← @asset_check: VWAP deviation, price consistency, freshness, fill rate
│   │
│   ├── resources/
│   │   ├── kafka_resource.py   ← KafkaConsumerResource: poll topics, commit offsets, get lag
│   │   ├── duckdb_resource.py  ← DuckDBResource: query Parquet via DuckDB SQL
│   │   └── storage_resource.py ← StorageResource: read/write Parquet (local or S3/MinIO)
│   │
│   ├── sensors/
│   │   ├── kafka_sensor.py     ← kafka_lag_sensor: fires bronze_job when lag > threshold
│   │   └── freshness_sensor.py ← gold_freshness_sensor: alerts when gold data is stale
│   │
│   ├── schedules/
│   │   └── schedules.py        ← daily_bronze_schedule (midnight), hourly_silver_schedule
│   │
│   ├── jobs/
│   │   └── jobs.py             ← bronze_job, silver_job, gold_job, full_pipeline_job
│   │
│   ├── io_managers/
│   │   └── parquet_io_manager.py ← ParquetIOManager: daily-partitioned Parquet save/load
│   │
│   └── utils/
│       ├── schemas.py          ← Pydantic v2 models matching mock-stock's exact field names
│       ├── ws_to_kafka.py      ← WebSocket → Kafka bridge (connects to mock-stock)
│       └── simulator.py        ← Fallback Python WS simulator (use mock-stock instead)
│
├── tests/
│   ├── conftest.py             ← Shared fixtures: sample DataFrames, mock resources
│   ├── test_assets/
│   │   ├── test_bronze_trades.py
│   │   ├── test_silver_trades.py
│   │   └── test_gold_ohlcv.py
│   └── test_checks/
│       └── test_quality_checks.py
│
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── provisioning/datasources/prometheus.yml
│       ├── provisioning/dashboards/dashboard.yml
│       └── dashboards/stock_pipeline.json
│
├── docker-compose.yml
├── pyproject.toml
├── dagster.yaml                ← Local Dagster SQLite config
└── .env.example                ← All environment variables documented
```

---

## Companion project

`mock-stock` lives at `../mock-stock` relative to this repo.
It is the **event source** — a Rust WebSocket server that simulates a full order book exchange.

```
../mock-stock/
├── src/                        ← Rust source
├── config/
│   ├── default.toml            ← Default single-symbol config
│   └── pipeline.toml           ← Multi-symbol config for this pipeline (7 symbols)
└── Dockerfile
```

Run it with: `cargo run --release -- config/pipeline.toml`

---

## Data flow (end-to-end)

```
mock-stock (Rust, ws://localhost:8080)
    │
    │  {"event_type": "TradeExecuted", "data": {...}}
    ▼
ws_to_kafka.py
    │  Injects event_type into data dict, routes to Kafka topic by event_type
    ▼
Kafka Topics
    stock.trades    ← TradeExecuted
    stock.orders    ← OrderPlaced + OrderCancelled
    stock.quotes    ← QuoteUpdate
    stock.orderbook ← OrderBookSnapshot
    stock.stats     ← MarketStats
    stock.halts     ← TradingHalt + TradingResume
    stock.agents    ← AgentAction
    │
    ▼
Bronze assets (Dagster)
    Poll Kafka → pd.DataFrame → write Parquet (./data/bronze/<table>/year=.../month=.../day=.../data.parquet)
    │
    ▼
Silver assets (Dagster)
    Read bronze Parquet → clean/type/validate → write Parquet (./data/silver/...)
    │
    ▼
Gold assets (Dagster, DuckDB SQL)
    Read silver + bronze Parquet → aggregate → write Parquet (./data/gold/...)
```

---

## Key commands

### Install
```bash
pip install -e ".[dev]"
```

### Run tests
```bash
pytest tests/ -v
pytest tests/test_assets/ -v          # assets only
pytest tests/test_checks/ -v          # checks only
pytest -k "test_silver" -v            # filter by name
```

### Lint / format
```bash
ruff check dagster_stock/ tests/       # lint
ruff format dagster_stock/ tests/      # format
mypy dagster_stock/                    # type check
```

### Dagster dev server
```bash
dagster dev -f dagster_stock/definitions.py
# Opens UI at http://localhost:3000
```

### Materialise assets via CLI
```bash
# All assets
dagster asset materialize -f dagster_stock/definitions.py --select "*"

# Single layer
dagster asset materialize -f dagster_stock/definitions.py --select "bronze/*"
dagster asset materialize -f dagster_stock/definitions.py --select "silver/*"
dagster asset materialize -f dagster_stock/definitions.py --select "gold/*"

# Single asset
dagster asset materialize -f dagster_stock/definitions.py --select "bronze_trades"
```

### Run the bridge
```bash
# Start mock-stock first (in ../mock-stock)
python dagster_stock/utils/ws_to_kafka.py
```

### Docker
```bash
docker compose up -d               # full stack
docker compose up -d kafka minio   # infra only (for local dev)
docker compose logs -f ws-bridge   # watch bridge logs
docker compose down -v             # tear down + remove volumes
```

---

## Environment variables (full reference)

Copy `.env.example` to `.env` and adjust. Key variables:

| Variable | Default | Purpose |
|---|---|---|
| `WS_URL` | `ws://localhost:8080` | mock-stock WebSocket address |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka cluster |
| `KAFKA_GROUP_ID` | `dagster-stock-pipeline` | Consumer group |
| `STORAGE_BACKEND` | `local` | `local` or `s3` |
| `LOCAL_DATA_DIR` | `./data` | Root for local Parquet files |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO/S3 endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `STORAGE_BUCKET` | `stock-lake` | S3 bucket name |
| `DUCKDB_PATH` | `:memory:` | DuckDB database path |
| `DUCKDB_THREADS` | `4` | DuckDB thread count |
| `DAGSTER_HOME` | `.dagster` | Dagster local state dir |
| `TOPIC_TRADES` | `stock.trades` | Override Kafka topic |
| `TOPIC_ORDERS` | `stock.orders` | Override Kafka topic |
| `TOPIC_QUOTES` | `stock.quotes` | Override Kafka topic |
| `TOPIC_ORDERBOOK` | `stock.orderbook` | Override Kafka topic |
| `TOPIC_STATS` | `stock.stats` | Override Kafka topic |
| `TOPIC_HALTS` | `stock.halts` | Override Kafka topic |
| `TOPIC_AGENTS` | `stock.agents` | Override Kafka topic |
| `RECONNECT_DELAY_S` | `5` | Bridge reconnect back-off |
| `LOG_INTERVAL_N` | `500` | Bridge: log every N events |
| `SKIP_UNKNOWN_EVENTS` | `false` | Drop events not in topic map |

---

## mock-stock event schemas

Every WS message: `{"event_type": "<Type>", "data": {...}}`
Welcome (on connect): `{"type": "welcome", "message": "...", "timestamp": "..."}` — bridge skips this.

| event_type | Key fields in `data` |
|---|---|
| `TradeExecuted` | `trade_id`, `symbol`, `price`, `quantity`, `is_aggressive_buy`, `buyer_agent_id`, `seller_agent_id`, `buy_order_id`, `sell_order_id`, `timestamp` |
| `OrderPlaced` | `order_id`, `symbol`, `side` (Buy/Sell), `order_type` (Market/Limit/StopLoss/StopLimit), `price` (null for Market), `quantity`, `agent_id`, `agent_type` (RetailTrader/InstitutionalInvestor/MarketMaker/HighFrequencyTrader), `timestamp` |
| `OrderCancelled` | `order_id`, `symbol`, `reason`, `timestamp` |
| `QuoteUpdate` | `symbol`, `best_bid`, `best_ask`, `best_bid_size`, `best_ask_size`, `spread`, `timestamp` |
| `OrderBookSnapshot` | `symbol`, `bids` (list of {price,quantity,order_count}), `asks` (list), `timestamp` |
| `MarketStats` | `symbol`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`, `timestamp` |
| `TradingHalt` | `symbol`, `reason`, `circuit_breaker_level` (Level1/Level2/Level3/null), `reference_price`, `current_price`, `price_change_percent`, `timestamp` |
| `TradingResume` | `symbol`, `halt_duration_ms`, `timestamp` |
| `AgentAction` | `agent_id`, `agent_type`, `symbol`, `action`, `decision_factors` (list), `timestamp` |

---

## Silver layer field normalisation

| Bronze field | Silver field | Notes |
|---|---|---|
| `best_bid` | `bid` | Renamed in silver_quotes |
| `best_ask` | `ask` | Renamed in silver_quotes |
| `best_bid_size` | `bid_size` | Renamed in silver_quotes |
| `best_ask_size` | `ask_size` | Renamed in silver_quotes |
| `is_aggressive_buy` | `side` | Derived: True→"buy", False→"sell" in silver_trades |
| `side` (Buy/Sell) | `side` (buy/sell) | Lowercased in silver_orders |
| `order_type` (Market/Limit/…) | `order_type` (market/limit/…) | Lowercased + remapped in silver_orders |
| `agent_type` (RetailTrader/…) | `agent_type` (retail/institutional/market_maker/hft) | Abbreviated in silver_orders |
| — | `fill_status` | Derived: "open" (OrderPlaced) / "cancelled" (OrderCancelled) in silver_orders |
| — | `mid_price` | Derived: (bid+ask)/2 in silver_quotes |
| — | `spread_bps` | Derived: (ask-bid)/mid_price×10000 in silver_quotes |
| — | `depth_imbalance` | Derived: (bid_size-ask_size)/(bid_size+ask_size) in silver_quotes |

---

## Adding a new asset

### Bronze asset
1. Create `dagster_stock/assets/bronze/<name>.py` following the pattern in `trades.py`.
2. Set `TOPIC = "stock.<topic>"` and a new `@asset(name="bronze_<name>", ...)`.
3. Export it in `dagster_stock/assets/bronze/__init__.py`.
4. Definitions auto-discovers via `load_assets_from_modules([bronze], group_name="bronze")` — no change to `definitions.py` needed.

### Silver asset
1. Create `dagster_stock/assets/silver/<name>.py`.
2. Add `deps=[bronze_<name>]` to read from the bronze Parquet.
3. Export in `dagster_stock/assets/silver/__init__.py`.

### Gold asset
1. Create `dagster_stock/assets/gold/<name>.py`.
2. Use `duckdb.query(sql)` with `read_parquet('{path}/**/*.parquet', hive_partitioning=true)`.
3. Export in `dagster_stock/assets/gold/__init__.py`.

### Asset checks
Add `@asset_check(asset=<asset_fn>, ...)` functions to `silver_checks.py` or `gold_checks.py` and append to the `silver_checks` / `gold_checks` list at the bottom.

---

## Jobs, schedules, and sensors

| Name | Type | Trigger | What it runs |
|---|---|---|---|
| `bronze_job` | Job | On-demand / schedule / sensor | All `bronze/*` assets |
| `silver_job` | Job | On-demand / schedule | All `silver/*` assets |
| `gold_job` | Job | On-demand | All `gold/*` assets |
| `full_pipeline_job` | Job | On-demand | All bronze + silver + gold |
| `daily_bronze_schedule` | Schedule | Midnight UTC | `bronze_job` |
| `hourly_silver_schedule` | Schedule | Top of every hour | `silver_job` |
| `kafka_lag_sensor` | Sensor | Every 60s | Fires `bronze_job` if any topic lag > 500 msgs |
| `gold_freshness_sensor` | Sensor | Every 30 min | Logs alert if gold data > 25h old |

---

## Conventions

- **Schema-on-read at bronze**: store events exactly as received from Kafka; never coerce types in bronze assets.
- **Schema-on-write at silver**: cast types, reject invalids, write rejected rows to `./data/rejected/<table>/`.
- **DuckDB for gold**: all gold aggregations use `duckdb.query(sql)` with `read_parquet(…, hive_partitioning=true)`.
- **Parquet partitioning**: `year=YYYY/month=MM/day=DD/data.parquet` written by `StorageResource.write_parquet`.
- **Event type discrimination**: the `event_type` column (injected by the bridge) distinguishes sub-types on shared topics (e.g. `OrderPlaced` vs `OrderCancelled` both on `stock.orders`).
- **No side effects in tests**: use `mock_storage` fixture (writes to `tmp_path`); patch `read_parquet` / `write_parquet` where needed.
- **Line length**: 100 characters (ruff enforced).
- **Python version**: 3.11+.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bronze_trades` returns empty DataFrame | Kafka has no messages / mock-stock not running | Start mock-stock, then the bridge |
| `silver_trades` has 0 rows after dedup | `event_type` column missing from bronze | Bridge not injecting event_type; check `ws_to_kafka.py` |
| Gold DuckDB query errors `no such file` | Silver Parquet not written yet | Materialise silver assets first |
| `KafkaException: Broker: Unknown topic` | Topics not auto-created | Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` (already in docker-compose) |
| Dagster can't import `dagster_stock` | Package not installed | Run `pip install -e ".[dev]"` |
| `DUCKDB_PATH` conflicts | Concurrent DuckDB writers on same file | Use `:memory:` (default) for concurrent runs |
