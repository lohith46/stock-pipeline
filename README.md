# Stock Exchange Data Pipeline

A production-style **Bronze → Silver → Gold** data pipeline built with **Dagster**, **Kafka**, **DuckDB**, and **MinIO/S3**, containerised with **Docker Compose**.

---

## Architecture

```
[WebSocket Feed]
      │
      ▼  (ws_to_kafka.py)
[Kafka Topics]
      │
      ├── stock.trades ──► bronze_trades ──► silver_trades ──┬──► gold_ohlcv
      │                                                       ├──► gold_agent_pnl
      │                                                       └──► gold_market_quality
      ├── stock.orders ──► bronze_orders ──► silver_orders ──► gold_agent_pnl
      ├── stock.quotes ──► bronze_quotes ──► silver_quotes ──► gold_market_quality
      ├── stock.stats  ──► bronze_market_stats ─────────────► gold_ohlcv
      └── stock.halts  ──► bronze_trading_halts ─────────────► gold_circuit_breaker
```

---

## Quickstart

### Prerequisites

- Docker & Docker Compose
- Python 3.11+

### 1. Start the stack

```bash
docker compose up -d
```

| Service          | URL                        |
|------------------|---------------------------|
| Dagster UI       | http://localhost:3010      |
| Grafana          | http://localhost:3000      |
| Prometheus       | http://localhost:9090      |
| MinIO Console    | http://localhost:9001      |
| Kafka            | localhost:9092             |

### 2. Start the WebSocket → Kafka bridge

```bash
pip install -e ".[dev]"
python dagster_stock/utils/ws_to_kafka.py
```

### 3. Run the pipeline

Open the Dagster UI at http://localhost:3010 and materialise assets, or run via CLI:

```bash
dagster asset materialize -f dagster_stock/definitions.py --select "*"
```

---

## Local development (without Docker)

```bash
pip install -e ".[dev]"

# Start Dagster dev server
dagster dev -f dagster_stock/definitions.py
```

Set the following environment variables (or create a `.env` file):

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
STORAGE_BUCKET=stock-lake
STORAGE_BACKEND=local   # or s3
LOCAL_DATA_DIR=./data
```

---

## Testing

```bash
pytest tests/ -v
```

---

## Key design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Orchestration | Dagster | Software-defined assets map cleanly to Bronze/Silver/Gold layers |
| Transformation | Python (pandas) | Flexible for ML feature derivation; replaces dbt SQL models |
| Storage format | Parquet + daily partitions | Columnar reads, partition pruning, works locally and on S3 |
| Local warehouse | DuckDB | Queries Parquet directly, no server needed |
| Message broker | Kafka | Decouples simulator from pipeline; enables replay |
| Data quality | @asset_check | Native Dagster checks for simple rules |
| Observability | Prometheus + Grafana | Standard stack; Dagster exports metrics natively |

---

## Project structure

See [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for the full annotated directory tree.
