"""
Anomaly Detector — Z-score based spike and slow-creep detection.
Author: Shailesh Chalke — Senior Snowflake Consultant

ALGORITHMS:
1. Z-Score: (daily_credits - rolling_mean) / rolling_std
2. Spike Detection: |z_score| > 3.0 on any single day
3. Slow Creep: 7+ consecutive days with positive daily delta
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)

SPIKE_Z_THRESHOLD   = 3.0    # z > 3.0 = statistically significant spike
CREEP_WINDOW_DAYS   = 7      # consecutive positive days = slow creep
ROLLING_WINDOW_7D   = 7      # 7-day rolling window for z-score
ROLLING_WINDOW_30D  = 30     # 30-day rolling window for trend


class AnomalyDetector:
    """
    Detects two classes of cost anomalies:
    1. Spikes: sudden single-day cost explosions (z > 3.0)
    2. Slow Creep: gradual unnoticed increases (7+ consecutive positive days)
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn  = connector
        self._mode = self._detect_mode()

    def _detect_mode(self) -> str:
        try:
            self.conn.query_to_df(
                "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1"
            )
            return "account_usage"
        except Exception:
            return "sample"

    # ─────────────────────────────────────────
    # TIMESERIES WITH Z-SCORE
    # ─────────────────────────────────────────
    def get_timeseries_with_zscore(self, days: int = 28) -> pd.DataFrame:
        """
        Return daily aggregated credit usage with 7-day rolling z-score.
        Columns: usage_date, total_credits, rolling_mean_7d, rolling_std_7d,
                 z_score, daily_delta
        """
        if self._mode == "account_usage":
            sql = f"""
                SELECT
                    DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                    SUM(credits_used)                    AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATEADD('DAY', -{days + ROLLING_WINDOW_7D}, CURRENT_DATE())
                  AND start_time <  CURRENT_TIMESTAMP()
                GROUP BY 1
                ORDER BY 1
            """
        else:
            sql = f"""
                SELECT
                    usage_date,
                    SUM(total_credits) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATEADD('DAY', -{days + ROLLING_WINDOW_7D}, CURRENT_DATE())
                GROUP BY 1
                ORDER BY 1
            """

        df = self.conn.query_to_df(sql)
        if df.empty:
            return pd.DataFrame()

        df["usage_date"]    = pd.to_datetime(df["usage_date"])
        df                  = df.sort_values("usage_date").reset_index(drop=True)
        df["total_credits"] = pd.to_numeric(df["total_credits"], errors="coerce").fillna(0)

        # 7-day rolling statistics
        df["rolling_mean_7d"] = (
            df["total_credits"].rolling(window=ROLLING_WINDOW_7D, min_periods=3).mean()
        )
        df["rolling_std_7d"]  = (
            df["total_credits"].rolling(window=ROLLING_WINDOW_7D, min_periods=3).std()
        )

        # Z-score: (value - mean) / std; avoid division by zero
        df["z_score"] = np.where(
            df["rolling_std_7d"] > 0,
            (df["total_credits"] - df["rolling_mean_7d"]) / df["rolling_std_7d"],
            0.0,
        )

        # Daily delta (day-over-day change)
        df["daily_delta"] = df["total_credits"].diff()

        # 30-day rolling mean for trend context
        df["rolling_mean_30d"] = (
            df["total_credits"].rolling(window=ROLLING_WINDOW_30D, min_periods=7).mean()
        )

        # Return only the requested days (trim warm-up period)
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df     = df[df["usage_date"] >= cutoff].reset_index(drop=True)

        return df

    # ─────────────────────────────────────────
    # SPIKE DETECTION
    # ─────────────────────────────────────────
    def detect_spikes(self) -> List[Dict[str, Any]]:
        """
        Return list of spike events where z-score > SPIKE_Z_THRESHOLD (3.0).
        Each spike includes warehouse_name, date, credits, z_score.
        """
        if self._mode == "account_usage":
            sql = f"""
                WITH daily_wh AS (
                    SELECT
                        DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                        warehouse_name,
                        SUM(credits_used)                    AS total_credits
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE start_time >= DATEADD('DAY', -35, CURRENT_DATE())
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT
                        warehouse_name,
                        usage_date,
                        total_credits,
                        AVG(total_credits) OVER (
                            PARTITION BY warehouse_name
                            ORDER BY usage_date
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_mean,
                        STDDEV(total_credits) OVER (
                            PARTITION BY warehouse_name
                            ORDER BY usage_date
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_std
                    FROM daily_wh
                )
                SELECT
                    warehouse_name,
                    usage_date,
                    total_credits,
                    rolling_mean,
                    rolling_std,
                    CASE
                        WHEN rolling_std > 0
                        THEN (total_credits - rolling_mean) / rolling_std
                        ELSE 0
                    END AS z_score
                FROM stats
                WHERE CASE
                        WHEN rolling_std > 0
                        THEN ABS((total_credits - rolling_mean) / rolling_std)
                        ELSE 0
                      END > {SPIKE_Z_THRESHOLD}
                  AND usage_date >= DATEADD('DAY', -28, CURRENT_DATE())
                ORDER BY z_score DESC
            """
        else:
            sql = f"""
                WITH daily_wh AS (
                    SELECT
                        usage_date,
                        warehouse_name,
                        SUM(total_credits) AS total_credits
                    FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                    WHERE usage_date >= DATEADD('DAY', -35, CURRENT_DATE())
                    GROUP BY 1, 2
                ),
                stats AS (
                    SELECT
                        warehouse_name,
                        usage_date,
                        total_credits,
                        AVG(total_credits) OVER (
                            PARTITION BY warehouse_name
                            ORDER BY usage_date
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_mean,
                        STDDEV(total_credits) OVER (
                            PARTITION BY warehouse_name
                            ORDER BY usage_date
                            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                        ) AS rolling_std
                    FROM daily_wh
                )
                SELECT
                    warehouse_name,
                    usage_date,
                    total_credits,
                    rolling_mean,
                    rolling_std,
                    CASE
                        WHEN rolling_std > 0
                        THEN (total_credits - rolling_mean) / rolling_std
                        ELSE 0
                    END AS z_score
                FROM stats
                WHERE CASE
                        WHEN rolling_std > 0
                        THEN ABS((total_credits - rolling_mean) / rolling_std)
                        ELSE 0
                      END > {SPIKE_Z_THRESHOLD}
                  AND usage_date >= DATEADD('DAY', -28, CURRENT_DATE())
                ORDER BY z_score DESC
            """

        df = self.conn.query_to_df(sql)
        if df.empty:
            return []

        df["usage_date"] = pd.to_datetime(df["usage_date"])
        return df.to_dict("records")

    # ─────────────────────────────────────────
    # SLOW CREEP DETECTION
    # ─────────────────────────────────────────
    def detect_slow_creep(self) -> List[Dict[str, Any]]:
        """
        Detect warehouses with 7+ consecutive days of increasing costs.
        This catches gradual query regression invisible to single-day z-score.
        """
        if self._mode == "account_usage":
            sql = """
                WITH daily_wh AS (
                    SELECT
                        DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                        warehouse_name,
                        SUM(credits_used)                    AS total_credits
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                    WHERE start_time >= DATEADD('DAY', -35, CURRENT_DATE())
                    GROUP BY 1, 2
                ),
                with_delta AS (
                    SELECT
                        warehouse_name,
                        usage_date,
                        total_credits,
                        LAG(total_credits) OVER (
                            PARTITION BY warehouse_name ORDER BY usage_date
                        ) AS prev_credits,
                        total_credits - LAG(total_credits) OVER (
                            PARTITION BY warehouse_name ORDER BY usage_date
                        ) AS daily_delta
                    FROM daily_wh
                ),
                with_sign AS (
                    SELECT
                        warehouse_name,
                        usage_date,
                        total_credits,
                        daily_delta,
                        CASE WHEN daily_delta > 0 THEN 1 ELSE 0 END AS is_increasing
                    FROM with_delta
                    WHERE prev_credits IS NOT NULL
                ),
                with_streak AS (
                    SELECT
                        warehouse_name,
                        usage_date,
                        total_credits,
                        daily_delta,
                        is_increasing,
                        SUM(CASE WHEN is_increasing = 0 THEN 1 ELSE 0 END) OVER (
                            PARTITION BY warehouse_name ORDER BY usage_date
                            ROWS UNBOUNDED PRECEDING
                        ) AS streak_group
                    FROM with_sign
                ),
                streak_lengths AS (
                    SELECT
                        warehouse_name,
                        MAX(usage_date)       AS streak_end_date,
                        MIN(usage_date)       AS streak_start_date,
                        COUNT(*)              AS consecutive_days,
                        SUM(daily_delta)      AS total_credit_increase
                    FROM with_streak
                    WHERE is_increasing = 1
                    GROUP BY warehouse_name, streak_group
                    HAVING COUNT(*) >= 7
                )
                SELECT
                    warehouse_name,
                    streak_start_date,
                    streak_end_date,
                    consecutive_days,
                    ROUND(total_credit_increase, 2) AS total_credit_increase
                FROM streak_lengths
                ORDER BY consecutive_days DESC
            """
        else:
            # Sample data version using Python-side streak detection
            sql = """
                SELECT
                    usage_date,
                    warehouse_name,
                    SUM(total_credits) AS total_credits
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATEADD('DAY', -35, CURRENT_DATE())
                GROUP BY 1, 2
                ORDER BY warehouse_name, usage_date
            """
            df = self.conn.query_to_df(sql)
            if df.empty:
                return []
            return self._detect_creep_python(df)

        df = self.conn.query_to_df(sql)
        if df.empty:
            return []
        return df.to_dict("records")

    def _detect_creep_python(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Python-side slow creep detection for sample data mode.
        Finds 7+ consecutive days of increasing credits per warehouse.
        """
        df["usage_date"] = pd.to_datetime(df["usage_date"])
        df = df.sort_values(["warehouse_name", "usage_date"])

        results = []
        for wh, group in df.groupby("warehouse_name"):
            group    = group.reset_index(drop=True)
            deltas   = group["total_credits"].diff()
            streak   = 0
            start_idx = 0
            total_increase = 0.0

            for i in range(1, len(deltas)):
                if deltas.iloc[i] > 0:
                    if streak == 0:
                        start_idx      = i - 1
                        total_increase = 0.0
                    streak         += 1
                    total_increase += deltas.iloc[i]
                else:
                    if streak >= CREEP_WINDOW_DAYS:
                        results.append({
                            "warehouse_name":      wh,
                            "streak_start_date":   group["usage_date"].iloc[start_idx],
                            "streak_end_date":     group["usage_date"].iloc[i - 1],
                            "consecutive_days":    streak,
                            "total_credit_increase": round(total_increase, 2),
                        })
                    streak = 0

            # Check streak at end of series
            if streak >= CREEP_WINDOW_DAYS:
                results.append({
                    "warehouse_name":      wh,
                    "streak_start_date":   group["usage_date"].iloc[start_idx],
                    "streak_end_date":     group["usage_date"].iloc[-1],
                    "consecutive_days":    streak,
                    "total_credit_increase": round(total_increase, 2),
                })

        return sorted(results, key=lambda x: x["consecutive_days"], reverse=True)