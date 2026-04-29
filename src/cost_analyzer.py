"""
Cost Analyzer - MTD, YTD, daily trends, idle waste, cloud services.
Author: Shailesh Chalke
"""

import logging
from typing import Optional

import pandas as pd

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)


class CostAnalyzer:
    """
    Analyzes Snowflake credit consumption.
    Auto-detects mode: ACCOUNT_USAGE (production) or sample tables (demo).
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn  = connector
        self._mode = self._detect_mode()
        logger.info(f"CostAnalyzer: running in {self._mode} mode")

    def _detect_mode(self) -> str:
        try:
            self.conn.query_to_df(
                "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1"
            )
            return "account_usage"
        except Exception:
            return "sample"

    def get_mtd_cost(self) -> float:
        """Return month-to-date total credits consumed."""
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used), 0) AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        else:
            sql = """
                SELECT COALESCE(SUM(total_credits), 0) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        if df.empty:
            return 0.0
        return float(df["total_credits"].iloc[0])

    def get_ytd_cost(self) -> float:
        """Return year-to-date total credits consumed."""
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used), 0) AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATE_TRUNC('YEAR', CURRENT_DATE())
            """
        else:
            sql = """
                SELECT COALESCE(SUM(total_credits), 0) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATE_TRUNC('YEAR', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        if df.empty:
            return 0.0
        return float(df["total_credits"].iloc[0])

    def get_daily_cost_trend(self, days: int = 28) -> pd.DataFrame:
        """Return daily credit usage for last N days, by warehouse."""
        if self._mode == "account_usage":
            sql = f"""
                SELECT
                    DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                    warehouse_name,
                    SUM(credits_used)                   AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATEADD('DAY', -{days}, CURRENT_DATE())
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
        else:
            sql = f"""
                SELECT
                    usage_date,
                    warehouse_name,
                    SUM(total_credits) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATEADD('DAY', -{days}, CURRENT_DATE())
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
        try:
            return self.conn.query_to_df(sql)
        except Exception as e:
            logger.error(f"get_daily_cost_trend failed: {e}")
            return pd.DataFrame()

    def get_cloud_services_cost(self) -> float:
        """Return MTD cloud services credits (metadata, compilation overhead)."""
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used_cloud_services), 0) AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        else:
            sql = """
                SELECT COALESCE(SUM(cloud_services_credits), 0) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        try:
            df = self.conn.query_to_df(sql)
            if df.empty:
                return 0.0
            return float(df["total_credits"].iloc[0])
        except Exception as e:
            logger.error(f"get_cloud_services_cost failed: {e}")
            return 0.0

    def get_user_attribution(self) -> pd.DataFrame:
        """Return MTD credit attribution by user."""
        if self._mode == "account_usage":
            sql = """
                SELECT
                    user_name,
                    SUM(credits_used_cloud_services) AS total_credits,
                    COUNT(DISTINCT query_id)          AS query_count
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATE_TRUNC('MONTH', CURRENT_DATE())
                  AND user_name IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20
            """
        else:
            sql = """
                SELECT
                    user_name,
                    SUM(total_credits) AS total_credits,
                    SUM(query_count)   AS query_count
                FROM FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION
                WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20
            """
        try:
            return self.conn.query_to_df(sql)
        except Exception as e:
            logger.error(f"get_user_attribution failed: {e}")
            return pd.DataFrame()

    def get_idle_waste(self) -> float:
        """
        Return estimated MTD idle credits (hours billed with zero queries).
        Only available in account_usage mode.
        """
        if self._mode == "account_usage":
            sql = """
                WITH hourly AS (
                    SELECT
                        warehouse_name,
                        DATE_TRUNC('HOUR', start_time) AS billing_hour,
                        SUM(credits_used)              AS hour_credits
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE start_time >= DATE_TRUNC('MONTH', CURRENT_DATE())
                    GROUP BY 1, 2
                ),
                with_queries AS (
                    SELECT
                        h.warehouse_name,
                        h.billing_hour,
                        h.hour_credits,
                        COUNT(q.query_id) AS queries_in_hour
                    FROM hourly h
                    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
                        ON h.warehouse_name = q.warehouse_name
                       AND DATE_TRUNC('HOUR', q.start_time) = h.billing_hour
                    GROUP BY 1, 2, 3
                )
                SELECT COALESCE(SUM(CASE WHEN queries_in_hour = 0 THEN hour_credits ELSE 0 END), 0)
                    AS idle_credits
                FROM with_queries
            """
            try:
                df = self.conn.query_to_df(sql)
                if df.empty:
                    return 0.0
                return float(df["idle_credits"].iloc[0])
            except Exception as e:
                logger.error(f"get_idle_waste failed: {e}")
                return 0.0
        else:
            # Sample mode: estimate 35% of MTD as idle
            return self.get_mtd_cost() * 0.35

    @staticmethod
    def credits_to_usd(credits: float, price_per_credit: float = 3.00) -> float:
        """Convert credit count to USD."""
        return round(credits * price_per_credit, 2)