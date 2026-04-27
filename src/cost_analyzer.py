"""
Cost Analyzer — MTD/YTD cost, idle waste, cloud services, user attribution.
Supports both ACCOUNT_USAGE (production) and FINOPS_SAMPLE (demo) tables.
Author: Shailesh Chalke — Senior Snowflake Consultant
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)


class CostAnalyzer:
    """
    Analyzes Snowflake compute costs across multiple dimensions:
    - MTD / YTD aggregate costs
    - Daily cost trend (28-day rolling)
    - Idle/wasted compute detection
    - Cloud services cost monitoring
    - Per-user credit attribution
    """

    # Snowflake: 1 credit = 3600 seconds of a single-node XSMALL
    # Size multipliers relative to X-SMALL
    SIZE_CREDIT_MULTIPLIERS = {
        "X-SMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8,
        "X-LARGE": 16, "2X-LARGE": 32, "3X-LARGE": 64, "4X-LARGE": 128,
    }

    def __init__(self, connector: SnowflakeConnector):
        self.conn   = connector
        self._mode  = self._detect_mode()
        logger.info(f"CostAnalyzer: running in {self._mode} mode")

    def _detect_mode(self) -> str:
        """
        Auto-detect whether ACCOUNT_USAGE or FINOPS_SAMPLE tables are available.
        Returns 'account_usage' or 'sample'.
        """
        try:
            self.conn.query_to_df(
                "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1"
            )
            return "account_usage"
        except Exception:
            return "sample"

    # ─────────────────────────────────────────
    # MTD COST
    # ─────────────────────────────────────────
    def get_mtd_cost(self) -> float:
        """Return total credits consumed month-to-date."""
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used), 0) AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', start_time) = DATE_TRUNC('MONTH', CURRENT_DATE())
                  AND start_time < CURRENT_TIMESTAMP()
            """
        else:
            sql = """
                SELECT COALESCE(SUM(total_credits), 0) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', usage_date) = DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        return float(df["total_credits"].iloc[0]) if not df.empty else 0.0

    # ─────────────────────────────────────────
    # YTD COST
    # ─────────────────────────────────────────
    def get_ytd_cost(self) -> float:
        """Return total credits consumed year-to-date."""
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used), 0) AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE DATE_TRUNC('YEAR', start_time) = DATE_TRUNC('YEAR', CURRENT_DATE())
                  AND start_time < CURRENT_TIMESTAMP()
            """
        else:
            sql = """
                SELECT COALESCE(SUM(total_credits), 0) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE DATE_TRUNC('YEAR', usage_date) = DATE_TRUNC('YEAR', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        return float(df["total_credits"].iloc[0]) if not df.empty else 0.0

    # ─────────────────────────────────────────
    # DAILY TREND
    # ─────────────────────────────────────────
    def get_daily_cost_trend(self, days: int = 28) -> pd.DataFrame:
        """
        Return daily credit consumption for the last N days.
        Columns: usage_date, warehouse_name, total_credits
        """
        if self._mode == "account_usage":
            sql = f"""
                SELECT
                    DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                    warehouse_name,
                    SUM(credits_used)                    AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATEADD('DAY', -{days}, CURRENT_DATE())
                  AND start_time <  CURRENT_TIMESTAMP()
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
        df = self.conn.query_to_df(sql)
        if not df.empty:
            df["usage_date"] = pd.to_datetime(df["usage_date"])
        return df

    # ─────────────────────────────────────────
    # IDLE WASTE
    # ─────────────────────────────────────────
    def get_idle_waste(self) -> float:
        """
        Estimate credits wasted on idle warehouses this month.
        Idle = credits billed with zero query activity.
        """
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used_cloud_services), 0) AS idle_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', start_time) = DATE_TRUNC('MONTH', CURRENT_DATE())
                  AND credits_used_compute > 0
                  AND credits_used_cloud_services / NULLIF(credits_used_compute, 0) > 0.1
            """
        else:
            sql = """
                SELECT COALESCE(SUM(idle_credits), 0) AS idle_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', usage_date) = DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        return float(df["idle_credits"].iloc[0]) if not df.empty else 0.0

    # ─────────────────────────────────────────
    # CLOUD SERVICES COST
    # ─────────────────────────────────────────
    def get_cloud_services_cost(self) -> float:
        """
        Return cloud services credits consumed MTD.
        Snowflake guideline: cloud services > 10% of compute = investigate.
        """
        if self._mode == "account_usage":
            sql = """
                SELECT COALESCE(SUM(credits_used_cloud_services), 0) AS cloud_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', start_time) = DATE_TRUNC('MONTH', CURRENT_DATE())
                  AND start_time < CURRENT_TIMESTAMP()
            """
        else:
            sql = """
                SELECT COALESCE(SUM(cloud_services_credits), 0) AS cloud_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE DATE_TRUNC('MONTH', usage_date) = DATE_TRUNC('MONTH', CURRENT_DATE())
            """
        df = self.conn.query_to_df(sql)
        return float(df["cloud_credits"].iloc[0]) if not df.empty else 0.0

    # ─────────────────────────────────────────
    # USER ATTRIBUTION
    # ─────────────────────────────────────────
    def get_user_attribution(self) -> pd.DataFrame:
        """
        Return per-user credit consumption for this month.
        Columns: user_name, total_credits, query_count
        """
        if self._mode == "account_usage":
            sql = """
                SELECT
                    qh.user_name,
                    SUM(wmh.credits_used)       AS total_credits,
                    COUNT(DISTINCT qh.query_id) AS query_count
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
                JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
                    ON qh.warehouse_name = wmh.warehouse_name
                    AND DATE_TRUNC('HOUR', qh.start_time) = DATE_TRUNC('HOUR', wmh.start_time)
                WHERE DATE_TRUNC('MONTH', qh.start_time) = DATE_TRUNC('MONTH', CURRENT_DATE())
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
                WHERE DATE_TRUNC('MONTH', usage_date) = DATE_TRUNC('MONTH', CURRENT_DATE())
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20
            """
        return self.conn.query_to_df(sql)

    # ─────────────────────────────────────────
    # HELPER: Credit → USD
    # ─────────────────────────────────────────
    @staticmethod
    def credits_to_usd(credits: float, price_per_credit: float = 3.00) -> float:
        """Convert credit count to USD. Default $3.00/credit (on-demand)."""
        return round(credits * price_per_credit, 2)