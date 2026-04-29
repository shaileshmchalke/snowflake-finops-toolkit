"""
Snowflake Connector - Password and Key-Pair authentication support.
Author: Shailesh Chalke
"""

import os
import logging
from typing import Optional

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """
    Snowflake connector with password and key-pair authentication.
    Supports query_to_df() for SELECT and execute_ddl() for DDL/DML.
    """

    def __init__(self):
        self._conn = self._connect()
        self._apply_session_parameters()

    def _connect(self) -> snowflake.connector.SnowflakeConnection:
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
            passphrase_str   = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", None)
            passphrase_bytes = passphrase_str.encode() if passphrase_str else None
            with open(private_key_path, "rb") as key_file:
                private_key = load_pem_private_key(key_file.read(), password=passphrase_bytes)
            connect_kwargs["private_key"] = private_key.private_bytes(
                encoding=Encoding.DER,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
            logger.info("Using key-pair authentication")
        else:
            connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
            logger.info("Using password authentication")

        conn = snowflake.connector.connect(**connect_kwargs)
        logger.info(f"Connected: account={account}, database={database}")
        return conn

    def _apply_session_parameters(self):
        # FIX: cursor explicitly close करतो — memory leak prevent
        params = {
            "QUERY_TAG":                    "FINOPS_TOOLKIT",
            "STATEMENT_TIMEOUT_IN_SECONDS": "300",
            "LOCK_TIMEOUT":                 "60",
            "USE_CACHED_RESULT":            "TRUE",
        }
        for param, value in params.items():
            cur = None
            try:
                cur = self._conn.cursor()
                cur.execute(f"ALTER SESSION SET {param} = {value}")
            except Exception as e:
                logger.warning(f"Could not set session param {param}: {e}")
            finally:
                if cur is not None:
                    cur.close()

    def query_to_df(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute SELECT query and return DataFrame."""
        try:
            cur = self._conn.cursor()
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            df = cur.fetch_pandas_all()
            cur.close()
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"query_to_df failed: {e} | SQL: {sql[:200]}")
            raise

    def execute_ddl(self, sql: str) -> bool:
        """Execute DDL/DML statements."""
        cur = None
        try:
            cur = self._conn.cursor()
            cur.execute(sql)
            logger.info(f"DDL executed: {sql[:100]}")
            return True
        except Exception as e:
            logger.error(f"execute_ddl failed: {e} | SQL: {sql[:200]}")
            raise
        finally:
            if cur is not None:
                cur.close()

    def execute_many(self, sql: str, data: list) -> bool:
        """Bulk insert using executemany."""
        cur = None
        try:
            cur = self._conn.cursor()
            cur.executemany(sql, data)
            return True
        except Exception as e:
            logger.error(f"execute_many failed: {e}")
            raise
        finally:
            if cur is not None:
                cur.close()

    def test_connection(self) -> bool:
        """Validate connection with lightweight query."""
        try:
            df = self.query_to_df("SELECT CURRENT_VERSION() AS version")
            logger.info(f"Snowflake version: {df['version'].iloc[0]}")
            return True
        except Exception:
            return False

    def close(self):
        """Close connection cleanly."""
        try:
            if self._conn and not self._conn.is_closed():
                self._conn.close()
                logger.info("Connection closed.")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")