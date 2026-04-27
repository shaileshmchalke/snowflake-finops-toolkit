"""
Snowflake Connector — Supports password auth and key-pair auth.
Author: Shailesh Chalke — Senior Snowflake Consultant
"""

import os
import logging
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """
    Production-grade Snowflake connector with:
    - Password authentication (default)
    - Key-pair authentication (optional, for service accounts)
    - Session parameter configuration
    - query_to_df() for SELECT queries
    - execute_ddl() for DDL/DML statements
    """

    def __init__(self):
        self._conn = self._connect()
        self._apply_session_parameters()

    # ─────────────────────────────────────────
    # CONNECTION
    # ─────────────────────────────────────────
    def _connect(self) -> snowflake.connector.SnowflakeConnection:
        """Build connection using env vars. Supports password + key-pair."""
        account   = os.environ["SNOWFLAKE_ACCOUNT"]
        user      = os.environ["SNOWFLAKE_USER"]
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "FINOPS_DEMO")
        schema    = os.environ.get("SNOWFLAKE_SCHEMA",    "FINOPS_SAMPLE")
        role      = os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN")

        private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")

        connect_kwargs = dict(
            account=account,
            user=user,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            client_session_keep_alive=True,
        )

        if private_key_path and os.path.exists(private_key_path):
            # Key-pair authentication
            private_key_passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", None)
            passphrase_bytes = (
                private_key_passphrase.encode() if private_key_passphrase else None
            )
            with open(private_key_path, "rb") as key_file:
                private_key = load_pem_private_key(
                    key_file.read(), password=passphrase_bytes
                )
            private_key_bytes = private_key.private_bytes(
                encoding=Encoding.DER,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
            connect_kwargs["private_key"] = private_key_bytes
            logger.info("Snowflake: using key-pair authentication")
        else:
            # Password authentication
            connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
            logger.info("Snowflake: using password authentication")

        conn = snowflake.connector.connect(**connect_kwargs)
        logger.info(f"Snowflake: connected to account={account}, db={database}")
        return conn

    def _apply_session_parameters(self):
        """Apply FinOps-optimized session parameters."""
        params = {
            "QUERY_TAG":               "FINOPS_TOOLKIT",
            "STATEMENT_TIMEOUT_IN_SECONDS": "300",
            "LOCK_TIMEOUT":            "60",
            "USE_CACHED_RESULT":       "TRUE",
        }
        for param, value in params.items():
            try:
                self._conn.cursor().execute(f"ALTER SESSION SET {param} = {value}")
            except Exception as e:
                logger.warning(f"Could not set session param {param}: {e}")

    # ─────────────────────────────────────────
    # QUERY METHODS
    # ─────────────────────────────────────────
    def query_to_df(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as a DataFrame.
        Uses Snowflake's fetch_pandas_all() for optimal performance.
        """
        try:
            cur = self._conn.cursor()
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            df = cur.fetch_pandas_all()
            # Normalize column names to lowercase for consistent access
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"query_to_df failed: {e}\nSQL: {sql[:200]}")
            raise

    def execute_ddl(self, sql: str) -> bool:
        """
        Execute DDL/DML statements (CREATE, ALTER, INSERT, etc.)
        Returns True on success.
        """
        try:
            self._conn.cursor().execute(sql)
            logger.info(f"DDL executed: {sql[:100]}")
            return True
        except Exception as e:
            logger.error(f"execute_ddl failed: {e}\nSQL: {sql[:200]}")
            raise

    def execute_many(self, sql: str, data: list) -> bool:
        """Bulk insert using executemany for performance."""
        try:
            self._conn.cursor().executemany(sql, data)
            return True
        except Exception as e:
            logger.error(f"execute_many failed: {e}")
            raise

    def test_connection(self) -> bool:
        """Validate connection with a lightweight query."""
        try:
            df = self.query_to_df("SELECT CURRENT_VERSION() AS version")
            logger.info(f"Snowflake version: {df['version'].iloc[0]}")
            return True
        except Exception:
            return False

    def close(self):
        """Close the connection cleanly."""
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()