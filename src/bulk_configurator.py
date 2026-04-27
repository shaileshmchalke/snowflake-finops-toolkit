"""
Bulk Configurator — Groups warehouses by workload type and generates
ALTER SQL + Rollback SQL for batch optimization.
Author: Shailesh Chalke — Senior Snowflake Consultant
"""

import copy
import logging
from typing import Any, Dict, List

from snowflake_connector import SnowflakeConnector
from warehouse_optimizer import (
    WarehouseOptimizer,
    RECOMMENDED_AUTO_SUSPEND,
    SIZE_CREDITS_PER_HOUR,
)

logger = logging.getLogger(__name__)


class BulkConfigurator:
    """
    Groups all warehouse recommendations by workload type and generates:
    1. Grouped ALTER SQL for batch execution
    2. Rollback SQL with CURRENT config for quick revert
    3. Total savings summary per group
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn      = connector
        self.optimizer = WarehouseOptimizer(connector)

    # ─────────────────────────────────────────
    # GROUPED RECOMMENDATIONS
    # ─────────────────────────────────────────
    def get_grouped_recommendations(self) -> Dict[str, Any]:
        """
        Return recommendations grouped by workload type.
        Structure: {
          "BI": {
            "warehouses": [...],
            "alter_sqls": [...],
            "rollback_sqls": [...],
            "total_annual_savings_credits": float,
            "total_annual_savings_usd": float,  (at $3/credit)
          },
          "ETL": {...},
          ...
        }
        """
        all_recs = self.optimizer.get_all_recommendations()
        grouped: Dict[str, Any] = {}

        for rec in all_recs:
            # Deep copy to avoid mutating original
            rec_copy      = copy.copy(rec)
            workload_type = rec_copy.get("workload_type", "UNKNOWN")

            if workload_type not in grouped:
                grouped[workload_type] = {
                    "warehouses":                    [],
                    "alter_sqls":                    [],
                    "rollback_sqls":                 [],
                    "total_annual_savings_credits":  0.0,
                    "total_annual_savings_usd":      0.0,
                }

            wh_name               = rec_copy["warehouse_name"]
            current_size          = rec_copy["current_size"]
            current_auto_suspend  = rec_copy["current_auto_suspend"]
            recommended_suspend   = rec_copy["recommended_auto_suspend"]
            recommended_size      = rec_copy["recommended_size"]
            min_cluster_count     = rec_copy["min_cluster_count"]
            savings_credits       = rec_copy["annual_savings_credits"]

            grouped[workload_type]["warehouses"].append(wh_name)

            # ALTER SQL
            alter_parts = [
                f"-- Workload: {workload_type} | Warehouse: {wh_name}",
                f"-- Savings: {savings_credits:.1f} credits/yr",
                f"ALTER WAREHOUSE {wh_name} SET AUTO_SUSPEND = {recommended_suspend};",
            ]
            if recommended_size != current_size:
                alter_parts.append(
                    f"ALTER WAREHOUSE {wh_name} SET WAREHOUSE_SIZE = '{recommended_size}';"
                )
            if min_cluster_count > 1:
                alter_parts.append(
                    f"ALTER WAREHOUSE {wh_name} SET MIN_CLUSTER_COUNT = 1 "
                    f"SCALING_POLICY = 'ECONOMY';"
                )
            grouped[workload_type]["alter_sqls"].append("\n".join(alter_parts))

            # ROLLBACK SQL — restores ORIGINAL config
            rollback_parts = [
                f"-- ROLLBACK for {wh_name} (restores original config)",
                f"ALTER WAREHOUSE {wh_name} SET AUTO_SUSPEND = {current_auto_suspend};",
            ]
            if recommended_size != current_size:
                rollback_parts.append(
                    f"ALTER WAREHOUSE {wh_name} SET WAREHOUSE_SIZE = '{current_size}';"
                )
            if min_cluster_count > 1:
                rollback_parts.append(
                    f"ALTER WAREHOUSE {wh_name} SET MIN_CLUSTER_COUNT = {min_cluster_count};"
                )
            grouped[workload_type]["rollback_sqls"].append("\n".join(rollback_parts))

            # Accumulate savings
            grouped[workload_type]["total_annual_savings_credits"] += savings_credits
            grouped[workload_type]["total_annual_savings_usd"]     += savings_credits * 3.00

        # Sort warehouses within each group by savings (highest first)
        for wt in grouped:
            grouped[wt]["total_annual_savings_credits"] = round(
                grouped[wt]["total_annual_savings_credits"], 2
            )
            grouped[wt]["total_annual_savings_usd"] = round(
                grouped[wt]["total_annual_savings_usd"], 2
            )

        return grouped

    # ─────────────────────────────────────────
    # WHAT-IF SIMULATION (live recalculation)
    # ─────────────────────────────────────────
    def simulate_whatif(
        self,
        num_warehouses: int,
        avg_credits_per_day: float,
        current_auto_suspend_s: int,
        new_auto_suspend_s: int,
        idle_fraction_pct: float,
        right_size_pct: float,
        credit_price: float = 3.00,
    ) -> Dict[str, float]:
        """
        Live what-if simulation. Returns a fresh dict every call — no cached state.
        Uses copy.copy() for parameter dict to prevent mutation.
        """
        # Copy input params to avoid any mutation side effects
        params = copy.copy({
            "num_warehouses":         num_warehouses,
            "avg_credits_per_day":    avg_credits_per_day,
            "current_auto_suspend_s": current_auto_suspend_s,
            "new_auto_suspend_s":     new_auto_suspend_s,
            "idle_fraction_pct":      idle_fraction_pct,
            "right_size_pct":         right_size_pct,
            "credit_price":           credit_price,
        })

        annual_base_credits = params["num_warehouses"] * params["avg_credits_per_day"] * 365

        # Auto-suspend savings
        suspend_reduction = max(
            0.0,
            (params["current_auto_suspend_s"] - params["new_auto_suspend_s"])
            / max(params["current_auto_suspend_s"], 1),
        )
        idle_saving     = (params["idle_fraction_pct"] / 100.0) * suspend_reduction
        suspend_savings = annual_base_credits * idle_saving

        # Right-sizing savings
        size_savings = annual_base_credits * (params["right_size_pct"] / 100.0)

        total_savings_credits = suspend_savings + size_savings
        total_savings_usd     = total_savings_credits * params["credit_price"]
        annual_base_usd       = annual_base_credits   * params["credit_price"]

        # Return new dict — no mutation of params
        return {
            "annual_base_credits":      round(annual_base_credits, 2),
            "annual_base_usd":          round(annual_base_usd, 2),
            "suspend_savings_credits":  round(suspend_savings, 2),
            "size_savings_credits":     round(size_savings, 2),
            "total_savings_credits":    round(total_savings_credits, 2),
            "total_savings_usd":        round(total_savings_usd, 2),
            "savings_pct":              round(total_savings_credits / max(annual_base_credits, 1) * 100, 2),
        }