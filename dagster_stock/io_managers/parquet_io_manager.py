"""
ParquetIOManager — custom Dagster IOManager that saves and loads DataFrames
as daily-partitioned Parquet files on local disk or S3/MinIO.

File layout:
    {base_dir}/{asset_key}/{year=YYYY}/{month=MM}/{day=DD}/data.parquet

The asset key is derived from the Dagster asset key (e.g. silver/trades →
path segment silver/trades).
"""

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


class ParquetIOManager(ConfigurableIOManager):
    base_dir: str = os.getenv("LOCAL_DATA_DIR", "./data")
    backend: str = os.getenv("STORAGE_BACKEND", "local")
    bucket: str = os.getenv("STORAGE_BUCKET", "stock-lake")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _asset_path(self, context: OutputContext | InputContext) -> str:
        key_parts = list(context.asset_key.path)
        now = datetime.now(tz=timezone.utc)
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        rel = "/".join(key_parts) + "/" + partition + "/data.parquet"
        if self.backend == "s3":
            return f"s3://{self.bucket}/{rel}"
        return str(Path(self.base_dir) / rel)

    def _s3fs(self):
        import s3fs
        return s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            endpoint_url=self.minio_endpoint,
        )

    # ── IOManager interface ─────────────────────────────────────────────────

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"ParquetIOManager expects a DataFrame, got {type(obj)}")

        path = self._asset_path(context)
        context.log.info("Writing %d rows → %s", len(obj), path)

        if self.backend == "s3":
            fs = self._s3fs()
            with fs.open(path, "wb") as f:
                obj.to_parquet(f, index=False)
        else:
            local_path = Path(path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            obj.to_parquet(local_path, index=False)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # Load the latest partition available
        key_parts = list(context.asset_key.path)
        rel_base = "/".join(key_parts)

        if self.backend == "s3":
            fs = self._s3fs()
            files = fs.glob(f"s3://{self.bucket}/{rel_base}/**/*.parquet")
            if not files:
                context.log.warning("No Parquet files found for %s", rel_base)
                return pd.DataFrame()
            return pd.concat([pd.read_parquet(fs.open(f)) for f in sorted(files)], ignore_index=True)
        else:
            base = Path(self.base_dir) / rel_base
            files = sorted(base.rglob("*.parquet"))
            if not files:
                context.log.warning("No Parquet files found at %s", base)
                return pd.DataFrame()
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
