"""
Warehouse Optimizer - Workload-aware auto-suspend, right-sizing, multi-cluster waste.
Author: Shailesh Chalke

Design decisions:
- Never hardcode warehouse size — always query live from data.
- Workload classification: BI=300s, ETL=120s, AD_HOC=60s, DS=600s.
- Every savings figure includes an exact calculation string.
- copy.copy() used when mutating recommendation objects.
"""

import copy
import logging
from typing import Any, Dict, List

import pandas as pd

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)

SIZE_ORDER = [
    "X-SMALL", "SMALL", "MEDIUM", "LARGE",
    "X-LARGE", "2X-LARGE", "3X-LARGE", "4X-LARGE",
]

# FIX: generate_sample_data.py सोबत consistent values — आधी mismatch होते
SIZE_CREDITS_PER_HOUR: Dict[str, int] = {
    "X-SMALL": 1,
    "SMALL":   2,
    "MEDIUM":  4,
    "LARGE":   8,
    "X-LARGE": 16,
    "2X-LARGE": 32,
    "3X-LARGE": 64,
    "4X-LARGE": 128,
}

WORKLOAD_KEYWORDS: Dict[str, List[str]] = {
    "BI":     ["bi", "report", "tableau", "power_bi", "looker", "dashboard", "analytics"],
    "ETL":    ["etl", "elt", "pipeline", "dbt", "fivetran", "airflow", "ingest", "load"],
    "DS":     ["ds", "ml", "science", "notebook", "jupyter", "model", "train"],
    "AD_HOC": ["adhoc", "ad_hoc", "sandbox", "dev", "test", "explore", "analyst"],
}

RECOMMENDED_AUTO_SUSPEND: Dict[str, int] = {
    "BI":      300,
    "ETL":     120,
    "AD_HOC":  60,
    "DS":      600,
    "UNKNOWN": 120,
}

LOW_UTILIZATION_THRESHOLD = 0.25
HIGH_SPILL_THRESHOLD      = 0.10


class WarehouseOptimizer:
    """
    Generates optimization recommendations for every warehouse.
    Uses live-queried sizes — never hardcoded values.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn  = connector
        self._mode = self._detect_mode()
        logger.info(f"WarehouseOptimizer: running in {self._mode} mode")

    def _detect_mode(self) -> str:
        try:
            self.conn.query_to_df(
                "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY LIMIT 1"
            )
            return "account_usage"
        except Exception:
            return "sample"

    def classify_workload(self, warehouse_name: str) -> str:
        """Classify warehouse into workload type by name keywords."""
        name_lower = warehouse_name.lower()
        for wl_type, keywords in WORKLOAD_KEYWORDS.items():
            if any(kw in name_lower for kw in keywords):
                return wl_type
        return "UNKNOWN"

    def _get_warehouse_inventory(self) -> pd.DataFrame:
        """
        Fetch warehouse metadata from live configuration.
        FIX: account_usage mode ORDER BY column fixed to credits_28d (col 6).
        """
        if self._mode == "account_usage":
            sql = """
                SELECT DISTINCT
                    w.warehouse_name,
                    w.warehouse_size        AS current_size,
                    w.auto_suspend          AS current_auto_suspend,
                    w.min_cluster_count,
                    w.max_cluster_count,
                    SUM(h.credits_used)     AS credits_28d,
                    AVG(h.credits_used)     AS avg_credits_per_hour
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES w
                LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY h
                    ON w.warehouse_name = h.warehouse_name
                   AND h.start_time >= DATEADD('DAY', -28, CURRENT_DATE())
                WHERE w.deleted IS NULL
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY 6 DESC
            """
        else:
            sql = """
                SELECT
                    warehouse_name,
                    warehouse_size           AS current_size,
                    auto_suspend             AS current_auto_suspend,
                    min_cluster_count,
                    max_cluster_count,
                    SUM(total_credits)       AS credits_28d,
                    AVG(total_credits)       AS avg_credits_per_hour
                FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
                WHERE usage_date >= DATEADD('DAY', -28, CURRENT_DATE())
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY 6 DESC
            """
        return self.conn.query_to_df(sql)

    def _calc_auto_suspend_savings(
        self,
        warehouse_name: str,
        current_size: str,
        current_auto_suspend: int,
        credits_28d: float,
        workload_type: str,
    ) -> Dict[str, Any]:
        recommended_suspend  = RECOMMENDED_AUTO_SUSPEND.get(workload_type, 120)
        credits_per_hour     = SIZE_CREDITS_PER_HOUR.get(current_size, 4)

        if current_auto_suspend <= recommended_suspend:
            return {
                "savings_credits_annual":  0.0,
                "recommended_auto_suspend": recommended_suspend,
                "detail": (
                    f"Auto-suspend already optimal: current={current_auto_suspend}s, "
                    f"recommended={recommended_suspend}s — no change needed."
                ),
            }

        idle_sessions_per_day      = 4
        minutes_saved_per_session  = (current_auto_suspend - recommended_suspend) / 60
        hours_saved_per_day        = (idle_sessions_per_day * minutes_saved_per_session) / 60
        savings_credits_per_day    = hours_saved_per_day * credits_per_hour
        savings_credits_annual     = savings_credits_per_day * 365

        detail = (
            f"AUTO-SUSPEND SAVINGS\n"
            f"  Warehouse:            {warehouse_name}\n"
            f"  Current Size:         {current_size} ({credits_per_hour} cr/hr)\n"
            f"  Current Auto-Suspend: {current_auto_suspend}s\n"
            f"  Recommended Suspend:  {recommended_suspend}s (workload={workload_type})\n"
            f"  Suspend Reduction:    {current_auto_suspend - recommended_suspend}s "
            f"({minutes_saved_per_session:.1f} min/session)\n"
            f"  Idle Sessions/Day:    {idle_sessions_per_day} (conservative)\n"
            f"  Hours Saved/Day:      {idle_sessions_per_day} x {minutes_saved_per_session:.1f}min / 60 "
            f"= {hours_saved_per_day:.3f}h\n"
            f"  Credits Saved/Day:    {hours_saved_per_day:.3f}h x {credits_per_hour} = "
            f"{savings_credits_per_day:.3f} cr\n"
            f"  Credits Saved/Year:   {savings_credits_per_day:.3f} x 365 = "
            f"{savings_credits_annual:.1f} cr\n"
        )
        return {
            "savings_credits_annual":   round(savings_credits_annual, 2),
            "recommended_auto_suspend": recommended_suspend,
            "detail":                   detail,
        }

    def _calc_right_sizing_savings(
        self,
        warehouse_name: str,
        current_size: str,
        credits_28d: float,
        avg_credits_per_hour: float,
    ) -> Dict[str, Any]:
        if current_size not in SIZE_CREDITS_PER_HOUR:
            return {
                "savings_credits_annual": 0.0,
                "recommended_size":       current_size,
                "detail":                 "Unknown size — no recommendation.",
            }

        current_cph       = SIZE_CREDITS_PER_HOUR[current_size]
        utilization_ratio = avg_credits_per_hour / max(current_cph, 1)

        if utilization_ratio > LOW_UTILIZATION_THRESHOLD:
            return {
                "savings_credits_annual": 0.0,
                "recommended_size":       current_size,
                "detail": (
                    f"RIGHT-SIZING: No action.\n"
                    f"  Utilization = {avg_credits_per_hour:.3f} / {current_cph} "
                    f"= {utilization_ratio:.2%} (threshold: {LOW_UTILIZATION_THRESHOLD:.0%})\n"
                    f"  Warehouse is adequately sized."
                ),
            }

        current_idx = SIZE_ORDER.index(current_size) if current_size in SIZE_ORDER else -1
        if current_idx <= 0:
            return {
                "savings_credits_annual": 0.0,
                "recommended_size":       current_size,
                "detail":                 "Already at minimum size.",
            }

        recommended_size    = SIZE_ORDER[current_idx - 1]
        recommended_cph     = SIZE_CREDITS_PER_HOUR[recommended_size]

        if credits_28d > 0 and current_cph > 0:
            hours_per_28d  = credits_28d / current_cph
            hours_per_year = hours_per_28d * (365 / 28)
        else:
            hours_per_year = 0.0

        savings_cph_diff       = current_cph - recommended_cph
        savings_credits_annual = savings_cph_diff * hours_per_year

        detail = (
            f"RIGHT-SIZING SAVINGS\n"
            f"  Warehouse:        {warehouse_name}\n"
            f"  Current Size:     {current_size} ({current_cph} cr/hr)\n"
            f"  Recommended Size: {recommended_size} ({recommended_cph} cr/hr)\n"
            f"  Utilization:      {avg_credits_per_hour:.3f} / {current_cph} "
            f"= {utilization_ratio:.2%} (threshold < {LOW_UTILIZATION_THRESHOLD:.0%})\n"
            f"  Hours/Year:       {credits_28d:.1f} / {current_cph} x (365/28) "
            f"= {hours_per_year:.1f}h\n"
            f"  Credit Diff/Hr:   {current_cph} - {recommended_cph} = {savings_cph_diff}\n"
            f"  Annual Savings:   {savings_cph_diff} x {hours_per_year:.1f}h "
            f"= {savings_credits_annual:.1f} cr\n"
        )
        return {
            "savings_credits_annual": round(savings_credits_annual, 2),
            "recommended_size":       recommended_size,
            "detail":                 detail,
        }

    def _calc_multicluster_waste(
        self,
        warehouse_name: str,
        min_cluster_count: int,
        max_cluster_count: int,
        credits_28d: float,
        current_size: str,
    ) -> Dict[str, Any]:
        if min_cluster_count <= 1 or max_cluster_count <= 1:
            return {
                "savings_credits_annual": 0.0,
                "detail":                 "Single-cluster or min=1: no multi-cluster waste.",
            }

        current_cph     = SIZE_CREDITS_PER_HOUR.get(current_size, 4)
        wasted_clusters = min_cluster_count - 1
        idle_fraction   = 0.30

        hours_per_28d  = credits_28d / max(current_cph * min_cluster_count, 1)
        hours_per_year = hours_per_28d * (365 / 28)

        wasted_credits_annual = wasted_clusters * current_cph * hours_per_year * idle_fraction

        detail = (
            f"MULTI-CLUSTER WASTE\n"
            f"  Warehouse:      {warehouse_name}\n"
            f"  Current Size:   {current_size} ({current_cph} cr/hr/cluster)\n"
            f"  Min Clusters:   {min_cluster_count} (always running)\n"
            f"  Max Clusters:   {max_cluster_count}\n"
            f"  Wasted:         min - 1 = {wasted_clusters} clusters\n"
            f"  Hours/Year:     {hours_per_year:.1f}h\n"
            f"  Idle Fraction:  {idle_fraction:.0%} (conservative)\n"
            f"  Wasted Credits: {wasted_clusters} x {current_cph} x {hours_per_year:.1f}h "
            f"x {idle_fraction:.0%} = {wasted_credits_annual:.1f} cr\n"
            f"  Recommendation: Reduce min_cluster_count to 1, set SCALING_POLICY=ECONOMY\n"
        )
        return {
            "savings_credits_annual": round(wasted_credits_annual, 2),
            "detail":                 detail,
        }

    def _generate_alter_sql(
        self,
        warehouse_name: str,
        recommended_auto_suspend: int,
        recommended_size: str,
        current_size: str,
        current_auto_suspend: int,
        min_cluster_count: int,
    ) -> List[str]:
        sqls = [
            f"ALTER WAREHOUSE {warehouse_name} SET AUTO_SUSPEND = {recommended_auto_suspend};"
        ]
        if recommended_size != current_size:
            sqls.append(
                f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{recommended_size}';"
            )
        if min_cluster_count > 1:
            sqls.append(
                f"ALTER WAREHOUSE {warehouse_name} SET MIN_CLUSTER_COUNT = 1 "
                f"SCALING_POLICY = 'ECONOMY';"
            )
        return sqls

    def get_all_recommendations(self) -> List[Dict[str, Any]]:
        """
        Return optimization recommendations for every warehouse.
        Each dict is a deep copy — safe to mutate in UI.
        """
        inventory_df = self._get_warehouse_inventory()
        if inventory_df.empty:
            logger.warning("No warehouse inventory data found.")
            return []

        recommendations = []

        for _, row in inventory_df.iterrows():
            warehouse_name       = str(row.get("warehouse_name", "UNKNOWN"))
            # Never hardcode size — always read from data
            current_size         = str(row.get("current_size", "SMALL")).upper()
            current_auto_suspend = int(row.get("current_auto_suspend", 600))
            min_cluster_count    = int(row.get("min_cluster_count", 1))
            max_cluster_count    = int(row.get("max_cluster_count", 1))
            credits_28d          = float(row.get("credits_28d", 0) or 0)
            avg_credits_per_hour = float(row.get("avg_credits_per_hour", 0) or 0)

            workload_type = self.classify_workload(warehouse_name)

            suspend_result = self._calc_auto_suspend_savings(
                warehouse_name, current_size, current_auto_suspend,
                credits_28d, workload_type,
            )
            rightsize_result = self._calc_right_sizing_savings(
                warehouse_name, current_size, credits_28d, avg_credits_per_hour,
            )
            multicluster_result = self._calc_multicluster_waste(
                warehouse_name, min_cluster_count, max_cluster_count,
                credits_28d, current_size,
            )

            recommended_auto_suspend = suspend_result["recommended_auto_suspend"]
            recommended_size         = rightsize_result["recommended_size"]

            # FIX: max(0, ...) — negative savings possible edge case prevent
            total_savings = max(
                0.0,
                suspend_result["savings_credits_annual"]
                + rightsize_result["savings_credits_annual"]
                + multicluster_result["savings_credits_annual"],
            )

            issues = []
            if suspend_result["savings_credits_annual"] > 0:
                issues.append(
                    f"Auto-suspend too high: {current_auto_suspend}s "
                    f"(recommended: {recommended_auto_suspend}s)"
                )
            if rightsize_result["savings_credits_annual"] > 0:
                issues.append(
                    f"Over-provisioned: {current_size} "
                    f"(recommended: {recommended_size})"
                )
            if multicluster_result["savings_credits_annual"] > 0:
                issues.append(
                    f"Multi-cluster waste: min_cluster={min_cluster_count} "
                    f"(recommend: reduce to 1)"
                )

            alter_sql = self._generate_alter_sql(
                warehouse_name, recommended_auto_suspend, recommended_size,
                current_size, current_auto_suspend, min_cluster_count,
            )

            combined_detail = "\n\n".join([
                suspend_result["detail"],
                rightsize_result["detail"],
                multicluster_result["detail"],
            ])

            rec = {
                "warehouse_name":             warehouse_name,
                "workload_type":              workload_type,
                "current_size":               current_size,
                "recommended_size":           recommended_size,
                "current_auto_suspend":       current_auto_suspend,
                "recommended_auto_suspend":   recommended_auto_suspend,
                "min_cluster_count":          min_cluster_count,
                "max_cluster_count":          max_cluster_count,
                "credits_28d":                credits_28d,
                "annual_savings_credits":     round(total_savings, 2),
                "savings_calculation_detail": combined_detail,
                "issues":                     issues,
                "alter_sql":                  alter_sql,
            }
            recommendations.append(copy.deepcopy(rec))

        return recommendations