"""
Bulk Configurator - Grouped ALTER + Rollback SQL generation by workload type.
Author: Shailesh Chalke
"""

import copy
import logging
from typing import Any, Dict, List

from snowflake_connector import SnowflakeConnector
from warehouse_optimizer import WarehouseOptimizer

logger = logging.getLogger(__name__)


class BulkConfigurator:
    """
    Groups warehouse recommendations by workload type.
    Generates bulk ALTER SQL and corresponding rollback SQL.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.conn      = connector
        self.optimizer = WarehouseOptimizer(connector)

    def get_grouped_recommendations(
        self,
        credit_price: float = 3.00,
    ) -> Dict[str, Any]:
        """
        Return recommendations grouped by workload type.
        FIX: credit_price parameter used — no more hardcoded $3.00.
        """
        all_recs = self.optimizer.get_all_recommendations()
        if not all_recs:
            return {}

        grouped: Dict[str, Any] = {}

        for rec in all_recs:
            workload_type   = rec.get("workload_type", "UNKNOWN")
            savings_credits = rec.get("annual_savings_credits", 0.0)

            if workload_type not in grouped:
                grouped[workload_type] = {
                    "warehouses":                    [],
                    "alter_sqls":                    [],
                    "rollback_sqls":                 [],
                    "total_annual_savings_credits":  0.0,
                    "total_annual_savings_usd":      0.0,
                }

            # Deep copy to prevent mutation
            rec_copy = copy.deepcopy(rec)

            wh_name              = rec_copy["warehouse_name"]
            current_size         = rec_copy["current_size"]
            current_auto_suspend = rec_copy["current_auto_suspend"]
            recommended_suspend  = rec_copy["recommended_auto_suspend"]
            recommended_size     = rec_copy["recommended_size"]
            min_cluster_count    = rec_copy["min_cluster_count"]

            grouped[workload_type]["warehouses"].append(wh_name)

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

            rollback_parts = [
                f"-- ROLLBACK for {wh_name} — restores original config",
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

            # FIX: max(0, savings) — negative savings prevent
            clean_savings = max(0.0, savings_credits)
            grouped[workload_type]["total_annual_savings_credits"] += clean_savings
            # FIX: credit_price parameter वापरतो — hardcoded $3.00 नाही
            grouped[workload_type]["total_annual_savings_usd"] += clean_savings * credit_price

        for wt in grouped:
            grouped[wt]["total_annual_savings_credits"] = round(
                grouped[wt]["total_annual_savings_credits"], 2
            )
            grouped[wt]["total_annual_savings_usd"] = round(
                grouped[wt]["total_annual_savings_usd"], 2
            )

        return grouped