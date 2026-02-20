"""
DuckDBResource — analytical SQL over the Parquet data lake.

A single in-process DuckDB connection is used per materialisation.
"""

import os
from typing import Any

import duckdb
import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger


class DuckDBResource(ConfigurableResource):
    database_path: str = os.getenv("DUCKDB_PATH", ":memory:")
    threads: int = int(os.getenv("DUCKDB_THREADS", "4"))

    def _connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self.database_path)
        conn.execute(f"PRAGMA threads={self.threads}")
        # Enable S3 / MinIO support if configured
        minio_endpoint = os.getenv("MINIO_ENDPOINT")
        if minio_endpoint:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"""
                SET s3_endpoint   = '{minio_endpoint.replace("http://", "").replace("https://", "")}';
                SET s3_access_key_id     = '{os.getenv("MINIO_ACCESS_KEY", "")}';
                SET s3_secret_access_key = '{os.getenv("MINIO_SECRET_KEY", "")}';
                SET s3_use_ssl    = false;
                SET s3_url_style  = 'path';
            """)
        return conn

    def query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        log = get_dagster_logger()
        conn = self._connect()
        try:
            result = conn.execute(sql, params or []).df()
            log.debug("DuckDB query returned %d rows", len(result))
            return result
        finally:
            conn.close()

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        conn = self._connect()
        try:
            conn.execute(sql, params or [])
        finally:
            conn.close()
