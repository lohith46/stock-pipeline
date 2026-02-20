# Stock Exchange Data Pipeline — Project Structure

```
stock-pipeline/
│
├── docker-compose.yml              # Full local stack (Kafka, MinIO, Prometheus, Grafana, Dagster)
├── pyproject.toml                  # Python dependencies & Dagster entrypoint config
├── README.md                       # Setup & quickstart guide
│
├── dagster_stock/
│   │
│   ├── definitions.py              # ← Dagster entrypoint: registers all assets, resources, jobs, schedules
│   │
│   ├── assets/
│   │   │
│   │   ├── bronze/                 # Layer 1: Raw ingestion from Kafka (schema-on-read)
│   │   │   ├── __init__.py
│   │   │   ├── trades.py           # bronze_trades         ← stock.trades topic
│   │   │   ├── orders.py           # bronze_orders         ← stock.orders topic
│   │   │   ├── quotes.py           # bronze_quotes         ← stock.quotes topic
│   │   │   ├── market_stats.py     # bronze_market_stats   ← stock.stats topic
│   │   │   └── trading_halts.py    # bronze_trading_halts  ← stock.halts topic
│   │   │
│   │   ├── silver/                 # Layer 2: Cleaned, typed, validated (schema-on-write)
│   │   │   ├── __init__.py
│   │   │   ├── trades.py           # silver_trades         ← dedup, type cast, reject invalid rows
│   │   │   ├── quotes.py           # silver_quotes         ← spread_bps, mid_price, depth_imbalance
│   │   │   └── orders.py           # silver_orders         ← join agent_type, fill status
│   │   │
│   │   └── gold/                   # Layer 3: Business aggregates (analytics-ready)
│   │       ├── __init__.py
│   │       ├── ohlcv.py            # gold_ohlcv                ← daily Open/High/Low/Close/Volume + VWAP per symbol
│   │       ├── market_quality.py   # gold_market_quality       ← avg spread, depth, quality score per symbol
│   │       ├── agent_pnl.py        # gold_agent_pnl            ← P&L, fill rate, slippage by agent type
│   │       └── circuit_breaker.py  # gold_circuit_breaker      ← halt frequency, recovery time, pre/post analysis
│   │
│   ├── checks/                     # @asset_check functions — attached data quality checks
│   │   ├── __init__.py
│   │   ├── silver_checks.py        # Uniqueness, nulls, price bounds, timestamp ordering
│   │   └── gold_checks.py          # VWAP deviation, cross-asset consistency, freshness SLA
│   │
│   ├── resources/                  # Shared infrastructure resources (injected into assets)
│   │   ├── __init__.py
│   │   ├── kafka_resource.py       # KafkaConsumerResource  — polls topics, commits offsets
│   │   ├── duckdb_resource.py      # DuckDBResource         — analytical SQL over Parquet lake
│   │   └── storage_resource.py     # StorageResource        — read/write Parquet to local or S3/MinIO
│   │
│   ├── sensors/                    # Event-driven pipeline triggers
│   │   ├── __init__.py
│   │   ├── kafka_sensor.py         # Triggers bronze assets when Kafka lag exceeds threshold
│   │   └── freshness_sensor.py     # Fires alert when gold assets exceed freshness SLA
│   │
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── schedules.py            # daily_bronze_schedule (midnight), hourly_silver_schedule
│   │
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── jobs.py                 # bronze_job, silver_job, gold_job, full_pipeline_job (asset selections)
│   │
│   ├── io_managers/
│   │   ├── __init__.py
│   │   └── parquet_io_manager.py   # Custom IOManager: saves/loads DataFrames as partitioned Parquet
│   │
│   └── utils/
│       ├── __init__.py
│       ├── ws_to_kafka.py          # WebSocket → Kafka bridge (run before Dagster)
│       └── schemas.py              # Pydantic models for each event type (type safety)
│
├── tests/
│   ├── conftest.py                 # Shared fixtures: sample DataFrames, mock resources
│   ├── test_assets/
│   │   ├── test_bronze_trades.py   # Unit tests: Kafka poll → DataFrame shape/types
│   │   ├── test_silver_trades.py   # Unit tests: dedup logic, rejection routing, derived fields
│   │   └── test_gold_ohlcv.py      # Unit tests: OHLCV correctness, VWAP formula
│   └── test_checks/
│       └── test_quality_checks.py  # Unit tests: each @asset_check passes/fails correctly
│
└── monitoring/
    ├── prometheus.yml              # Scrape config for Dagster + custom pipeline metrics
    └── grafana/
        ├── provisioning/           # Auto-provision datasources + dashboards on startup
        └── dashboards/
            └── stock_pipeline.json # Pre-built dashboard: event rates, lag, quality scores
```

## Asset Dependency Graph

```
[Kafka Topics]
     │
     ├── stock.trades ──► bronze_trades ──► silver_trades ──┬──► gold_ohlcv
     │                                                       ├──► gold_agent_pnl
     │                                                       └──► gold_market_quality
     │
     ├── stock.orders ──► bronze_orders ──► silver_orders ──► gold_agent_pnl
     │
     ├── stock.quotes ──► bronze_quotes ──► silver_quotes ──► gold_market_quality
     │
     ├── stock.stats  ──► bronze_market_stats ─────────────► gold_ohlcv (cross-validation)
     │
     └── stock.halts  ──► bronze_trading_halts ─────────────► gold_circuit_breaker
```

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Orchestration | Dagster | Software-defined assets map cleanly to Bronze/Silver/Gold layers; better local dev than Airflow |
| Transformation | Python (pandas) in @asset functions | Replaces dbt SQL models; more flexible for ML feature derivation |
| Storage format | Parquet + daily partitions | Columnar reads, partition pruning, works locally and on S3 |
| Local warehouse | DuckDB | Queries Parquet directly, no server needed, SQL interface for Gold layer |
| Message broker | Kafka | Decouples simulator from pipeline; enables replay and exactly-once semantics |
| Data quality | @asset_check + Great Expectations | Native Dagster checks for simple rules, GE for complex suites |
| Observability | Prometheus + Grafana | Standard stack; Dagster exports metrics natively |
