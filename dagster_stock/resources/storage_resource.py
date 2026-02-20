"""
StorageResource — read/write Parquet to local filesystem or S3/MinIO.

Environment variables:
    STORAGE_BACKEND   local | s3  (default: local)
    LOCAL_DATA_DIR    path for local storage (default: ./data)
    MINIO_ENDPOINT    http://minio:9000
    MINIO_ACCESS_KEY  minioadmin
    MINIO_SECRET_KEY  minioadmin
    STORAGE_BUCKET    stock-lake
"""

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, ConfigurableResource, get_dagster_logger


class StorageResource(ConfigurableResource):
    backend: str = os.getenv("STORAGE_BACKEND", "local")
    local_data_dir: str = os.getenv("LOCAL_DATA_DIR", "./data")
    bucket: str = os.getenv("STORAGE_BUCKET", "stock-lake")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    def _partition_prefix(self) -> str:
        now = datetime.now(tz=timezone.utc)
        return f"year={now.year}/month={now.month:02d}/day={now.day:02d}"

    def get_parquet_path(self, layer: str, table: str) -> str:
        if self.backend == "s3":
            return f"s3://{self.bucket}/{layer}/{table}"
        return str(Path(self.local_data_dir) / layer / table)

    def write_parquet(self, df: pd.DataFrame, layer: str, table: str, context: AssetExecutionContext | None = None) -> str:
        log = get_dagster_logger()
        partition = self._partition_prefix()

        if self.backend == "s3":
            import s3fs
            fs = s3fs.S3FileSystem(
                key=self.access_key,
                secret=self.secret_key,
                endpoint_url=self.minio_endpoint,
            )
            path = f"s3://{self.bucket}/{layer}/{table}/{partition}/data.parquet"
            with fs.open(path, "wb") as f:
                df.to_parquet(f, index=False)
        else:
            out_dir = Path(self.local_data_dir) / layer / table / partition
            out_dir.mkdir(parents=True, exist_ok=True)
            path = str(out_dir / "data.parquet")
            df.to_parquet(path, index=False)

        log.info("Wrote %d rows → %s", len(df), path)
        return path

    def read_parquet(self, layer: str, table: str) -> pd.DataFrame:
        log = get_dagster_logger()
        base = self.get_parquet_path(layer, table)

        if self.backend == "s3":
            import s3fs
            fs = s3fs.S3FileSystem(
                key=self.access_key,
                secret=self.secret_key,
                endpoint_url=self.minio_endpoint,
            )
            files = fs.glob(f"{base}/**/*.parquet")
            if not files:
                log.warning("No Parquet files found at s3://%s", base)
                return pd.DataFrame()
            return pd.concat([pd.read_parquet(fs.open(f)) for f in files], ignore_index=True)
        else:
            path = Path(base)
            files = list(path.rglob("*.parquet"))
            if not files:
                log.warning("No Parquet files found at %s", path)
                return pd.DataFrame()
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
