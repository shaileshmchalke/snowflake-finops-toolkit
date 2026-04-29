"""
Anomaly Detector - Z-score spike detection and slow creep analysis.
Author: Shailesh Chalke
"""

import logging
from typing import List, Dict, Any

import pandas as pd
import numpy as np

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)

Z_SCORE_THRESHOLD    = 3.0
CREEP_WINDOW_DAYS    = 7
ROLLING_WINDOW_DAYS  = 7


class AnomalyDetector:
    """
    Detects cost anomalies using z-score analysis and slow-creep pattern detection.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn  = connector
        self._mode = self._detect_mode()
        logger.info(f"AnomalyDetector: running in {self._mode} mode")

    def _detect_mode(self) -> str:
        try:
            self.conn.query_to_df(
                "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1"
            )
            return "account_usage"
        except Exception:
            return "sample"

    def _get_daily_credits(self, days: int = 28) -> pd.DataFrame:
        """Fetch daily credit totals per warehouse."""
        if self._mode == "account_usage":
            sql = f"""
                SELECT
                    DATE_TRUNC('DAY', start_time)::DATE AS usage_date,
                    warehouse_name,
                    SUM(credits_used)                   AS total_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE start_time >= DATEADD('DAY', -{days}, CURRENT_DATE())
                GROUP BY 1, 2
                ORDER BY 2, 1
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
                ORDER BY 2, 1
            """
        try:
            return self.conn.query_to_df(sql)
        except Exception as e:
            logger.error(f"_get_daily_credits failed: {e}")
            return pd.DataFrame()

    def get_timeseries_with_zscore(self, days: int = 28) -> pd.DataFrame:
        """
        Return daily credits with z-score for each warehouse.
        FIX: Division by zero prevented — std=0 case handled.
        """
        df = self._get_daily_credits(days)
        if df.empty:
            return pd.DataFrame()

        results = []
        for wh_name, group in df.groupby("warehouse_name"):
            wh_df = group.copy().sort_values("usage_date").reset_index(drop=True)
            credits = wh_df["total_credits"].values

            mean_val = np.mean(credits)
            std_val  = np.std(credits)

            # FIX: std=0 হे divide by zero prevent — flat line warehouses साठी z_score=0
            if std_val > 0:
                z_scores = (credits - mean_val) / std_val
            else:
                z_scores = np.zeros(len(credits))

            wh_df["z_score"]         = z_scores
            wh_df["rolling_mean"]    = pd.Series(credits).rolling(
                window=ROLLING_WINDOW_DAYS, min_periods=1
            ).mean().values
            wh_df["warehouse_name"]  = wh_name
            results.append(wh_df)

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def detect_spikes(self, days: int = 28) -> List[Dict[str, Any]]:
        """
        Return list of days where z-score exceeds threshold.
        Sorted by severity (highest z-score first).
        """
        ts_df = self.get_timeseries_with_zscore(days)
        if ts_df.empty:
            return []

        spike_df = ts_df[ts_df["z_score"].abs() >= Z_SCORE_THRESHOLD].copy()
        spike_df = spike_df.sort_values("z_score", ascending=False)

        spikes = []
        for _, row in spike_df.iterrows():
            spikes.append({
                "warehouse_name": row.get("warehouse_name", ""),
                "usage_date":     str(row.get("usage_date", "")),
                "total_credits":  float(row.get("total_credits", 0)),
                "z_score":        round(float(row.get("z_score", 0)), 2),
            })
        return spikes

    def detect_slow_creep(self, days: int = 28) -> List[Dict[str, Any]]:
        """
        Detect warehouses with N consecutive days of increasing credit usage.
        Catches gradual regression invisible to single-day z-score.
        """
        ts_df = self.get_timeseries_with_zscore(days)
        if ts_df.empty:
            return []

        results = []
        for wh_name, group in ts_df.groupby("warehouse_name"):
            wh_df  = group.sort_values("usage_date").reset_index(drop=True)
            credits = wh_df["total_credits"].values

            max_streak   = 0
            curr_streak  = 0
            streak_start = None
            best_start   = None

            for i in range(1, len(credits)):
                if credits[i] > credits[i - 1]:
                    if curr_streak == 0:
                        streak_start = i - 1
                    curr_streak += 1
                    if curr_streak > max_streak:
                        max_streak = curr_streak
                        best_start = streak_start
                else:
                    curr_streak = 0

            if max_streak >= CREEP_WINDOW_DAYS:
                start_val = credits[best_start] if best_start is not None else 0
                end_val   = credits[best_start + max_streak] if best_start is not None else 0
                increase  = end_val - start_val
                pct       = (increase / max(start_val, 0.001)) * 100

                results.append({
                    "warehouse_name":      wh_name,
                    "consecutive_days":    max_streak,
                    "total_increase_pct":  round(pct, 1),
                    "start_credits":       round(float(start_val), 2),
                    "end_credits":         round(float(end_val), 2),
                })

        results.sort(key=lambda x: x["consecutive_days"], reverse=True)
        return results