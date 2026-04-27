"""
Unit tests for WarehouseOptimizer.
Tests: savings calculation, workload classification, right-sizing, multi-cluster waste.
Author: Shailesh Chalke — Senior Snowflake Consultant

Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from warehouse_optimizer import (
    WarehouseOptimizer,
    SIZE_CREDITS_PER_HOUR,
    RECOMMENDED_AUTO_SUSPEND,
    SIZE_ORDER,
)


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────
@pytest.fixture
def mock_connector():
    conn = MagicMock()
    # Mode detection: raise to simulate sample mode
    conn.query_to_df.side_effect = _mock_warehouse_router
    return conn


def _mock_warehouse_router(sql: str, params=None) -> pd.DataFrame:
    """Return fake warehouse inventory data."""

    if "SELECT 1" in sql and "ACCOUNT_USAGE" in sql:
        raise Exception("No ACCOUNT_USAGE")

    # Warehouse inventory query
    if "warehouse_name" in sql and "current_size" in sql or "warehouse_size" in sql:
        return pd.DataFrame({
            "warehouse_name":       ["BI_ANALYTICS_WH", "ETL_PIPELINE_WH",
                                     "ADHOC_DEV_WH",    "DS_TRAINING_WH"],
            "current_size":         ["LARGE",            "X-LARGE",
                                     "MEDIUM",           "2X-LARGE"],
            "current_auto_suspend": [600,                600,
                                     900,                600],
            "min_cluster_count":    [2,                  1,
                                     1,                  3],
            "max_cluster_count":    [4,                  1,
                                     1,                  6],
            "credits_28d":          [320.0,              180.0,
                                     45.0,               850.0],
            "avg_credits_per_hour": [1.5,                2.0,
                                     0.5,                8.0],
        })

    return pd.DataFrame()


@pytest.fixture
def optimizer(mock_connector):
    return WarehouseOptimizer(mock_connector)


# ─────────────────────────────────────────────
# TEST: WORKLOAD CLASSIFICATION
# ─────────────────────────────────────────────
class TestWorkloadClassification:
    def test_bi_warehouse(self, optimizer):
        assert optimizer.classify_workload("BI_ANALYTICS_WH") == "BI"

    def test_etl_warehouse(self, optimizer):
        assert optimizer.classify_workload("ETL_PIPELINE_WH") == "ETL"

    def test_adhoc_warehouse(self, optimizer):
        assert optimizer.classify_workload("ADHOC_DEV_WH") == "AD_HOC"

    def test_ds_warehouse(self, optimizer):
        assert optimizer.classify_workload("DS_NOTEBOOKS_WH") == "DS"

    def test_report_warehouse(self, optimizer):
        assert optimizer.classify_workload("TABLEAU_REPORT_WH") == "BI"

    def test_dbt_warehouse(self, optimizer):
        assert optimizer.classify_workload("DBT_TRANSFORM_WH") == "ETL"

    def test_unknown_warehouse(self, optimizer):
        assert optimizer.classify_workload("PROD_WH_01") == "UNKNOWN"

    def test_case_insensitive(self, optimizer):
        """Classification should be case-insensitive."""
        assert optimizer.classify_workload("bi_reporting_wh") == "BI"


# ─────────────────────────────────────────────
# TEST: AUTO-SUSPEND SAVINGS
# ─────────────────────────────────────────────
class TestAutoSuspendSavings:
    def test_high_suspend_bi_generates_savings(self, optimizer):
        """BI warehouse with 600s suspend should save vs recommended 300s."""
        result = optimizer._calc_auto_suspend_savings(
            "BI_WH", "LARGE", 600, 320.0, "BI"
        )
        assert result["savings_credits_annual"] > 0
        assert result["recommended_auto_suspend"] == 300

    def test_optimal_suspend_no_savings(self, optimizer):
        """Already optimal auto-suspend should return 0 savings."""
        result = optimizer._calc_auto_suspend_savings(
            "ETL_WH", "LARGE", 120, 180.0, "ETL"
        )
        assert result["savings_credits_annual"] == 0.0
        assert result["recommended_auto_suspend"] == 120

    def test_adhoc_900s_suspend_high_savings(self, optimizer):
        """AD_HOC with 900s suspend should generate significant savings vs 60s."""
        result = optimizer._calc_auto_suspend_savings(
            "ADHOC_WH", "MEDIUM", 900, 45.0, "AD_HOC"
        )
        assert result["savings_credits_annual"] > 0
        assert result["recommended_auto_suspend"] == 60

    def test_detail_string_present(self, optimizer):
        """Savings detail should contain key calculation terms."""
        result = optimizer._calc_auto_suspend_savings(
            "TEST_WH", "MEDIUM", 600, 100.0, "ETL"
        )
        assert "AUTO-SUSPEND" in result["detail"]
        assert "credits/hr" in result["detail"]

    def test_savings_are_non_negative(self, optimizer):
        """Savings should never be negative."""
        result = optimizer._calc_auto_suspend_savings(
            "TEST_WH", "MEDIUM", 60, 100.0, "BI"
        )
        assert result["savings_credits_annual"] >= 0.0

    def test_recommended_suspend_matches_workload(self, optimizer):
        """Recommended suspend should match workload-specific value."""
        for workload, expected_suspend in RECOMMENDED_AUTO_SUSPEND.items():
            result = optimizer._calc_auto_suspend_savings(
                f"{workload}_WH", "MEDIUM", 3600, 100.0, workload
            )
            assert result["recommended_auto_suspend"] == expected_suspend


# ─────────────────────────────────────────────
# TEST: RIGHT-SIZING SAVINGS
# ─────────────────────────────────────────────
class TestRightSizingSavings:
    def test_over_provisioned_generates_savings(self, optimizer):
        """
        X-LARGE warehouse (16 cr/hr) with avg_credits_per_hour = 0.5
        → utilization = 3.1% << 25% threshold → should downsize.
        """
        result = optimizer._calc_right_sizing_savings(
            "ADHOC_WH", "X-LARGE", 100.0, avg_credits_per_hour=0.5
        )
        assert result["savings_credits_annual"] > 0
        assert result["recommended_size"] != "X-LARGE"

    def test_well_utilized_no_savings(self, optimizer):
        """
        MEDIUM warehouse (4 cr/hr) with avg = 3.0 cr/hr
        → utilization = 75% >> 25% → no right-sizing.
        """
        result = optimizer._calc_right_sizing_savings(
            "ETL_WH", "MEDIUM", 200.0, avg_credits_per_hour=3.0
        )
        assert result["savings_credits_annual"] == 0.0
        assert result["recommended_size"] == "MEDIUM"

    def test_minimum_size_no_downsize(self, optimizer):
        """X-SMALL is already minimum — cannot downsize further."""
        result = optimizer._calc_right_sizing_savings(
            "SMALL_WH", "X-SMALL", 5.0, avg_credits_per_hour=0.1
        )
        assert result["savings_credits_annual"] == 0.0
        assert result["recommended_size"] == "X-SMALL"

    def test_recommended_size_is_one_tier_down(self, optimizer):
        """
        Right-sizing should recommend exactly one tier down, not multiple.
        LARGE (idx=3) → MEDIUM (idx=2) when under-utilized.
        """
        result = optimizer._calc_right_sizing_savings(
            "BI_WH", "LARGE", 320.0, avg_credits_per_hour=0.5
        )
        if result["savings_credits_annual"] > 0:
            current_idx     = SIZE_ORDER.index("LARGE")
            recommended_idx = SIZE_ORDER.index(result["recommended_size"])
            assert recommended_idx == current_idx - 1

    def test_detail_contains_utilization_ratio(self, optimizer):
        """Detail string must include utilization ratio."""
        result = optimizer._calc_right_sizing_savings(
            "TEST_WH", "LARGE", 100.0, avg_credits_per_hour=0.5
        )
        assert "Utilization" in result["detail"] or "utilization" in result["detail"].lower()


# ─────────────────────────────────────────────
# TEST: MULTI-CLUSTER WASTE
# ─────────────────────────────────────────────
class TestMultiClusterWaste:
    def test_min_cluster_2_generates_savings(self, optimizer):
        """
        min_cluster=2 with LARGE warehouse should generate waste savings.
        1 always-on extra cluster × 30% idle × full-year hours.
        """
        result = optimizer._calc_multicluster_waste(
            "BI_WH", min_cluster_count=2, max_cluster_count=4,
            credits_28d=320.0, current_size="LARGE"
        )
        assert result["savings_credits_annual"] > 0

    def test_min_cluster_1_no_waste(self, optimizer):
        """Single-cluster warehouse (min=1) should have zero multi-cluster waste."""
        result = optimizer._calc_multicluster_waste(
            "ETL_WH", min_cluster_count=1, max_cluster_count=1,
            credits_28d=180.0, current_size="X-LARGE"
        )
        assert result["savings_credits_annual"] == 0.0

    def test_min_cluster_3_larger_waste_than_min_2(self, optimizer):
        """More always-on clusters = more waste."""
        result_2 = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=2, max_cluster_count=4,
            credits_28d=400.0, current_size="LARGE"
        )
        result_3 = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=3, max_cluster_count=4,
            credits_28d=400.0, current_size="LARGE"
        )
        assert result_3["savings_credits_annual"] > result_2["savings_credits_annual"]

    def test_detail_mentions_reduce_min_cluster(self, optimizer):
        """Recommendation should suggest reducing min_cluster_count."""
        result = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=2, max_cluster_count=3,
            credits_28d=200.0, current_size="MEDIUM"
        )
        assert "min_cluster_count" in result["detail"]


# ─────────────────────────────────────────────
# TEST: FULL RECOMMENDATIONS
# ─────────────────────────────────────────────
class TestGetAllRecommendations:
    def test_returns_list(self, optimizer):
        """get_all_recommendations should return a list."""
        recs = optimizer.get_all_recommendations()
        assert isinstance(recs, list)

    def test_recommendation_count_matches_warehouses(self, optimizer):
        """Should return one recommendation per warehouse in mock data."""
        recs = optimizer.get_all_recommendations()
        assert len(recs) == 4  # 4 warehouses in mock data

    def test_recommendation_has_required_keys(self, optimizer):
        """Each recommendation must include all required fields."""
        required_keys = [
            "warehouse_name", "workload_type", "current_size",
            "recommended_size", "current_auto_suspend", "recommended_auto_suspend",
            "annual_savings_credits", "savings_calculation_detail",
            "issues", "alter_sql",
        ]
        recs = optimizer.get_all_recommendations()
        for rec in recs:
            for key in required_keys:
                assert key in rec, f"Missing key '{key}' in recommendation for {rec.get('warehouse_name')}"

    def test_no_hardcoded_medium_size(self, optimizer):
        """
        Critical: current_size must reflect actual data, not 'MEDIUM' hardcode.
        BI_ANALYTICS_WH should show 'LARGE', not 'MEDIUM'.
        """
        recs = optimizer.get_all_recommendations()
        bi_rec = next((r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH"), None)
        assert bi_rec is not None
        assert bi_rec["current_size"] == "LARGE"

    def test_etl_current_size_xlarge(self, optimizer):
        """ETL_PIPELINE_WH should reflect X-LARGE from mock data."""
        recs = optimizer.get_all_recommendations()
        etl_rec = next((r for r in recs if r["warehouse_name"] == "ETL_PIPELINE_WH"), None)
        assert etl_rec is not None
        assert etl_rec["current_size"] == "X-LARGE"

    def test_savings_are_non_negative(self, optimizer):
        """No recommendation should have negative savings."""
        recs = optimizer.get_all_recommendations()
        for rec in recs:
            assert rec["annual_savings_credits"] >= 0.0

    def test_bi_workload_classification(self, optimizer):
        """BI_ANALYTICS_WH must be classified as BI."""
        recs = optimizer.get_all_recommendations()
        bi_rec = next((r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH"), None)
        assert bi_rec["workload_type"] == "BI"

    def test_ds_workload_classification(self, optimizer):
        """DS_TRAINING_WH must be classified as DS."""
        recs = optimizer.get_all_recommendations()
        ds_rec = next((r for r in recs if r["warehouse_name"] == "DS_TRAINING_WH"), None)
        assert ds_rec["workload_type"] == "DS"

    def test_multi_cluster_warehouse_has_issues(self, optimizer):
        """BI_ANALYTICS_WH with min_cluster=2 should have multi-cluster in issues."""
        recs = optimizer.get_all_recommendations()
        bi_rec = next((r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH"), None)
        issues_text = " ".join(bi_rec["issues"]).lower()
        assert "multi-cluster" in issues_text or "cluster" in issues_text

    def test_alter_sql_generated(self, optimizer):
        """Every recommendation with savings should have ALTER SQL."""
        recs = optimizer.get_all_recommendations()
        for rec in recs:
            assert isinstance(rec["alter_sql"], list)
            assert len(rec["alter_sql"]) > 0