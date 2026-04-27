"""
Warehouse Optimizer — Workload-aware auto-suspend, right-sizing, multi-cluster waste.
Author: Shailesh Chalke — Senior Snowflake Consultant

KEY DESIGN DECISIONS:
- Never hardcode CURRENT_SIZE; always query it dynamically.
- Use copy.copy() when mutating recommendation objects.
- Workload classification: BI=300s, ETL=120s, AD_HOC=60s, DS=600s.
- Every savings figure includes an exact calculation string.
"""

import copy
import logging
import re
from typing import Any, Dict, List, Optional

import pandas as pd

from snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)


# Warehouse size ordering (index = relative credit weight)
SIZE_ORDER = [
    "X-SMALL", "SMALL", "MEDIUM", "LARGE",
    "X-LARGE", "2X-LARGE", "3X-LARGE", "4X-LARGE",
]

# Credits per hour per size (single cluster)
SIZE_CREDITS_PER_HOUR = {
    "X-SMALL": 1,  "SMALL": 2,   "MEDIUM": 4,   "LARGE": 8,
    "X-LARGE": 16, "2X-LARGE": 32, "3X-LARGE": 64, "4X-LARGE": 128,
}

# Workload detection keywords in warehouse name
WORKLOAD_KEYWORDS: Dict[str, List[str]] = {
    "BI":     ["bi", "report", "tableau", "power_bi", "looker", "dashboard", "analytics"],
    "ETL":    ["etl", "elt", "pipeline", "dbt", "fivetran", "airflow", "ingest", "load"],
    "DS":     ["ds", "ml", "science", "notebook", "jupyter", "model", "train"],
    "AD_HOC": ["adhoc", "ad_hoc", "sandbox", "dev", "test", "explore", "analyst"],
}

# Recommended auto-suspend by workload type (seconds)
RECOMMENDED_AUTO_SUSPEND: Dict[str, int] = {
    "BI":     300,   # 5 min — preserves result cache
    "ETL":    120,   # 2 min — batch workloads, fast suspend
    "AD_HOC": 60,    # 1 min — interactive, minimize idle
    "DS":     600,   # 10 min — long-running notebooks
    "UNKNOWN": 120,  # safe default
}

# Right-sizing thresholds
LOW_UTILIZATION_THRESHOLD   = 0.25   # < 25% avg execution ratio → downsize
HIGH_SPILL_THRESHOLD        = 0.10   # > 10% queries with remote spill → upsize


class WarehouseOptimizer:
    """
    Analyzes each warehouse and generates actionable optimization recommendations.
    All savings calculations use actual queried sizes — never hardcoded values.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn = connector
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

    # ─────────────────────────────────────────
    # WORKLOAD CLASSIFICATION
    # ─────────────────────────────────────────
    def classify_workload(self, warehouse_name: str) -> str:
        """
        Classify warehouse into workload type by inspecting its name.
        Order matters: more specific keywords first.
        """
        name_lower = warehouse_name.lower()
        for wl_type, keywords in WORKLOAD_KEYWORDS.items():
            if any(kw in name_lower for kw in keywords):
                return wl_type
        return "UNKNOWN"

    # ─────────────────────────────────────────
    # WAREHOUSE INVENTORY
    # ─────────────────────────────────────────
    def _get_warehouse_inventory(self) -> pd.DataFrame:
        """
        Fetch warehouse metadata: name, current size, auto_suspend, cluster info.
        Always queries LIVE configuration — never hardcoded.
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
                ORDER BY 3 DESC
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

    # ─────────────────────────────────────────
    # AUTO-SUSPEND SAVINGS CALCULATION
    # ─────────────────────────────────────────
    def _calc_auto_suspend_savings(
        self,
        warehouse_name: str,
        current_size: str,
        current_auto_suspend: int,
        credits_28d: float,
        workload_type: str,
    ) -> Dict[str, Any]:
        """
        Calculate savings from optimizing auto-suspend.
        Returns: savings_credits_annual, detail string, recommended_auto_suspend.
        """
        recommended_suspend = RECOMMENDED_AUTO_SUSPEND.get(workload_type, 120)
        credits_per_hour    = SIZE_CREDITS_PER_HOUR.get(current_size, 4)

        if current_auto_suspend <= recommended_suspend:
            return {
                "savings_credits_annual": 0.0,
                "recommended_auto_suspend": recommended_suspend,
                "detail": (
                    f"Auto-suspend already optimal: current={current_auto_suspend}s, "
                    f"recommended={recommended_suspend}s — no change needed."
                ),
            }

        # Idle minutes saved per day = difference in suspend thresholds / 60 * estimated idle sessions
        # Conservative: assume 4 idle sessions per day per warehouse
        idle_sessions_per_day = 4
        minutes_saved_per_session = (current_auto_suspend - recommended_suspend) / 60
        hours_saved_per_day = (idle_sessions_per_day * minutes_saved_per_session) / 60

        savings_credits_per_day    = hours_saved_per_day * credits_per_hour
        savings_credits_annual     = savings_credits_per_day * 365

        detail = (
            f"AUTO-SUSPEND SAVINGS CALCULATION\n"
            f"  Warehouse:             {warehouse_name}\n"
            f"  Current Size:          {current_size} ({credits_per_hour} credits/hr)\n"
            f"  Current Auto-Suspend:  {current_auto_suspend}s\n"
            f"  Recommended Suspend:   {recommended_suspend}s (workload={workload_type})\n"
            f"  Suspend Reduction:     {current_auto_suspend - recommended_suspend}s "
            f"  ({minutes_saved_per_session:.1f} min/session)\n"
            f"  Idle Sessions/Day:     {idle_sessions_per_day} (conservative estimate)\n"
            f"  Hours Saved/Day:       {idle_sessions_per_day} × "
            f"  {minutes_saved_per_session:.1f}min ÷ 60 = {hours_saved_per_day:.3f}h\n"
            f"  Credits Saved/Day:     {hours_saved_per_day:.3f}h × "
            f"  {credits_per_hour} cr/hr = {savings_credits_per_day:.3f} credits\n"
            f"  Credits Saved/Year:    {savings_credits_per_day:.3f} × 365 "
            f"  = {savings_credits_annual:.1f} credits\n"
        )
        return {
            "savings_credits_annual":   round(savings_credits_annual, 2),
            "recommended_auto_suspend": recommended_suspend,
            "detail": detail,
        }

    # ─────────────────────────────────────────
    # RIGHT-SIZING SAVINGS
    # ─────────────────────────────────────────
    def _calc_right_sizing_savings(
        self,
        warehouse_name: str,
        current_size: str,
        credits_28d: float,
        avg_credits_per_hour: float,
    ) -> Dict[str, Any]:
        """
        Detect over-provisioned warehouses based on credit consumption patterns.
        Right-size down one tier if credits are very low for the current size.
        """
        if current_size not in SIZE_CREDITS_PER_HOUR:
            return {"savings_credits_annual": 0.0, "recommended_size": current_size, "detail": "Unknown size."}

        current_credits_per_hour = SIZE_CREDITS_PER_HOUR[current_size]

        # If avg usage is < 25% of the current size's capacity, recommend downsizing
        utilization_ratio = avg_credits_per_hour / max(current_credits_per_hour, 1)

        if utilization_ratio > LOW_UTILIZATION_THRESHOLD:
            return {
                "savings_credits_annual": 0.0,
                "recommended_size": current_size,
                "detail": (
                    f"RIGHT-SIZING: No action.\n"
                    f"  Utilization ratio = {avg_credits_per_hour:.3f} / {current_credits_per_hour} "
                    f"= {utilization_ratio:.2%} (threshold: {LOW_UTILIZATION_THRESHOLD:.0%})\n"
                    f"  Warehouse is adequately sized."
                ),
            }

        # Find recommended size: one tier down from current
        current_idx = SIZE_ORDER.index(current_size) if current_size in SIZE_ORDER else -1
        if current_idx <= 0:
            return {"savings_credits_annual": 0.0, "recommended_size": current_size,
                    "detail": "Already at minimum size."}

        recommended_size = SIZE_ORDER[current_idx - 1]
        recommended_cph  = SIZE_CREDITS_PER_HOUR[recommended_size]

        # Annual savings = (current_cph - recommended_cph) × estimated_hours_per_year
        # Estimate hours/year from 28-day data
        if credits_28d > 0 and current_credits_per_hour > 0:
            hours_per_28d   = credits_28d / current_credits_per_hour
            hours_per_year  = hours_per_28d * (365 / 28)
        else:
            hours_per_year  = 0

        savings_cph_diff        = current_credits_per_hour - recommended_cph
        savings_credits_annual  = savings_cph_diff * hours_per_year

        detail = (
            f"RIGHT-SIZING SAVINGS CALCULATION\n"
            f"  Warehouse:             {warehouse_name}\n"
            f"  Current Size:          {current_size} ({current_credits_per_hour} credits/hr)\n"
            f"  Recommended Size:      {recommended_size} ({recommended_cph} credits/hr)\n"
            f"  Utilization Ratio:     {avg_credits_per_hour:.3f} / {current_credits_per_hour} "
            f"= {utilization_ratio:.2%} (threshold < {LOW_UTILIZATION_THRESHOLD:.0%})\n"
            f"  Hours/Year (estimate): {credits_28d:.1f} cr ÷ {current_credits_per_hour} cr/hr "
            f"× (365/28) = {hours_per_year:.1f}h\n"
            f"  Credit Diff/Hr:        {current_credits_per_hour} - {recommended_cph} "
            f"= {savings_cph_diff} credits/hr\n"
            f"  Annual Savings:        {savings_cph_diff} × {hours_per_year:.1f}h "
            f"= {savings_credits_annual:.1f} credits/yr\n"
        )
        return {
            "savings_credits_annual": round(savings_credits_annual, 2),
            "recommended_size":       recommended_size,
            "detail":                 detail,
        }

    # ─────────────────────────────────────────
    # MULTI-CLUSTER WASTE
    # ─────────────────────────────────────────
    def _calc_multicluster_waste(
        self,
        warehouse_name: str,
        min_cluster_count: int,
        max_cluster_count: int,
        credits_28d: float,
        current_size: str,
    ) -> Dict[str, Any]:
        """
        Detect multi-cluster warehouses where min_cluster > 1 wastes credits.
        A min_cluster=2 means 2 clusters always running, even at zero load.
        """
        if min_cluster_count <= 1 or max_cluster_count <= 1:
            return {
                "savings_credits_annual": 0.0,
                "detail": "Single-cluster or min=1: no multi-cluster waste.",
            }

        current_cph = SIZE_CREDITS_PER_HOUR.get(current_size, 4)

        # Wasted clusters = (min_cluster_count - 1) always running
        wasted_clusters = min_cluster_count - 1

        # Estimate idle hours from 28-day data
        # Conservative: minimum clusters waste ~30% of their billed time idle
        idle_fraction = 0.30
        hours_per_28d = credits_28d / max(current_cph * min_cluster_count, 1)
        hours_per_year = hours_per_28d * (365 / 28)

        wasted_credits_annual = (
            wasted_clusters * current_cph * hours_per_year * idle_fraction
        )

        detail = (
            f"MULTI-CLUSTER WASTE CALCULATION\n"
            f"  Warehouse:             {warehouse_name}\n"
            f"  Current Size:          {current_size} ({current_cph} credits/hr/cluster)\n"
            f"  Min Clusters:          {min_cluster_count} (always running)\n"
            f"  Max Clusters:          {max_cluster_count}\n"
            f"  Wasted Clusters:       min - 1 = {wasted_clusters}\n"
            f"  Estimated Hours/Year:  {hours_per_year:.1f}h\n"
            f"  Idle Fraction:         {idle_fraction:.0%} (conservative)\n"
            f"  Wasted Credits/Year:   {wasted_clusters} × {current_cph} × "
            f"  {hours_per_year:.1f}h × {idle_fraction:.0%} "
            f"  = {wasted_credits_annual:.1f} credits\n"
            f"  Recommendation:        Reduce min_cluster_count to 1, "
            f"  set scaling_policy=ECONOMY\n"
        )
        return {
            "savings_credits_annual": round(wasted_credits_annual, 2),
            "detail":                 detail,
        }

    # ─────────────────────────────────────────
    # GENERATE ALTER SQL
    # ─────────────────────────────────────────
    def _generate_alter_sql(
        self,
        warehouse_name: str,
        recommended_auto_suspend: int,
        recommended_size: str,
        current_size: str,
        current_auto_suspend: int,
        min_cluster_count: int,
    ) -> List[str]:
        """Generate ALTER WAREHOUSE SQL commands for the recommendations."""
        sqls = []

        # Auto-suspend change
        sqls.append(
            f"ALTER WAREHOUSE {warehouse_name} SET AUTO_SUSPEND = {recommended_auto_suspend};"
        )

        # Size change only if different
        if recommended_size != current_size:
            sqls.append(
                f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{recommended_size}';"
            )

        # Multi-cluster: reduce min to 1
        if min_cluster_count > 1:
            sqls.append(
                f"ALTER WAREHOUSE {warehouse_name} SET MIN_CLUSTER_COUNT = 1 "
                f"SCALING_POLICY = 'ECONOMY';"
            )

        return sqls

    # ─────────────────────────────────────────
    # MAIN: GET ALL RECOMMENDATIONS
    # ─────────────────────────────────────────
    def get_all_recommendations(self) -> List[Dict[str, Any]]:
        """
        Return a list of optimization recommendations for every warehouse.
        Each recommendation dict is a deep copy — safe to mutate in UI.
        """
        inventory_df = self._get_warehouse_inventory()
        if inventory_df.empty:
            logger.warning("No warehouse inventory data found.")
            return []

        recommendations = []

        for _, row in inventory_df.iterrows():
            warehouse_name    = str(row.get("warehouse_name", "UNKNOWN"))
            # CRITICAL: always read size from data, never hardcode
            current_size      = str(row.get("current_size", "SMALL")).upper()
            current_auto_suspend = int(row.get("current_auto_suspend", 600))
            min_cluster_count = int(row.get("min_cluster_count", 1))
            max_cluster_count = int(row.get("max_cluster_count", 1))
            credits_28d       = float(row.get("credits_28d", 0) or 0)
            avg_credits_per_hour = float(row.get("avg_credits_per_hour", 0) or 0)

            # Normalize size
            if current_size not in SIZE_ORDER:
                current_size = "MEDIUM"  # fallback only if truly unknown

            workload_type = self.classify_workload(warehouse_name)

            # Calculate savings components
            suspend_result = self._calc_auto_suspend_savings(
                warehouse_name, current_size, current_auto_suspend,
                credits_28d, workload_type,
            )
            sizing_result = self._calc_right_sizing_savings(
                warehouse_name, current_size, credits_28d, avg_credits_per_hour,
            )
            cluster_result = self._calc_multicluster_waste(
                warehouse_name, min_cluster_count, max_cluster_count,
                credits_28d, current_size,
            )

            total_savings = (
                suspend_result["savings_credits_annual"]
                + sizing_result["savings_credits_annual"]
                + cluster_result["savings_credits_annual"]
            )

            recommended_size         = sizing_result.get("recommended_size", current_size)
            recommended_auto_suspend = suspend_result["recommended_auto_suspend"]

            # Build issues list
            issues = []
            if current_auto_suspend > recommended_auto_suspend:
                issues.append(
                    f"Auto-suspend too high: {current_auto_suspend}s "
                    f"(recommended: {recommended_auto_suspend}s for {workload_type})"
                )
            if recommended_size != current_size:
                issues.append(
                    f"Warehouse over-provisioned: {current_size} → downsize to {recommended_size}"
                )
            if min_cluster_count > 1:
                issues.append(
                    f"Multi-cluster min={min_cluster_count}: "
                    f"{min_cluster_count - 1} cluster(s) always running unnecessarily"
                )

            alter_sqls = self._generate_alter_sql(
                warehouse_name, recommended_auto_suspend, recommended_size,
                current_size, current_auto_suspend, min_cluster_count,
            )

            combined_detail = (
                suspend_result["detail"] + "\n\n"
                + sizing_result["detail"] + "\n\n"
                + cluster_result["detail"] + "\n\n"
                + f"TOTAL ANNUAL SAVINGS: {total_savings:.1f} credits"
            )

            # Deep copy to prevent mutation bugs across recommendations
            rec = copy.copy({
                "warehouse_name":             warehouse_name,
                "workload_type":              workload_type,
                "current_size":               current_size,
                "recommended_size":           recommended_size,
                "current_auto_suspend":       current_auto_suspend,
                "recommended_auto_suspend":   recommended_auto_suspend,
                "min_cluster_count":          min_cluster_count,
                "max_cluster_count":          max_cluster_count,
                "credits_28d":                round(credits_28d, 2),
                "annual_savings_credits":     round(total_savings, 2),
                "suspend_savings_credits":    suspend_result["savings_credits_annual"],
                "sizing_savings_credits":     sizing_result["savings_credits_annual"],
                "cluster_savings_credits":    cluster_result["savings_credits_annual"],
                "savings_calculation_detail": combined_detail,
                "issues":                     issues,
                "alter_sql":                  alter_sqls,
            })
            recommendations.append(rec)

        return recommendations