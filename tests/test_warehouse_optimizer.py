"""
Unit tests for WarehouseOptimizer.
Mock connector used — no live Snowflake connection needed.
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from warehouse_optimizer import WarehouseOptimizer, SIZE_ORDER, SIZE_CREDITS_PER_HOUR


MOCK_INVENTORY = pd.DataFrame([
    {
        "warehouse_name":       "BI_ANALYTICS_WH",
        "current_size":         "LARGE",
        "current_auto_suspend": 600,
        "min_cluster_count":    2,
        "max_cluster_count":    4,
        "credits_28d":          320.0,
        "avg_credits_per_hour": 0.5,
    },
    {
        "warehouse_name":       "ETL_PIPELINE_WH",
        "current_size":         "X-LARGE",
        "current_auto_suspend": 600,
        "min_cluster_count":    1,
        "max_cluster_count":    1,
        "credits_28d":          180.0,
        "avg_credits_per_hour": 12.0,
    },
    {
        "warehouse_name":       "ADHOC_ANALYST_WH",
        "current_size":         "MEDIUM",
        "current_auto_suspend": 900,
        "min_cluster_count":    1,
        "max_cluster_count":    1,
        "credits_28d":          20.0,
        "avg_credits_per_hour": 0.2,
    },
    {
        "warehouse_name":       "DS_TRAINING_WH",
        "current_size":         "2X-LARGE",
        "current_auto_suspend": 600,
        "min_cluster_count":    2,
        "max_cluster_count":    4,
        "credits_28d":          400.0,
        "avg_credits_per_hour": 25.0,
    },
])


def _mock_query_router(sql: str, params=None) -> pd.DataFrame:
    if "SNOWFLAKE.ACCOUNT_USAGE" in sql and "SELECT 1" in sql:
        raise Exception("Simulated: ACCOUNT_USAGE not available")
    if "WH_METERING_HISTORY" in sql and "GROUP BY" in sql:
        return MOCK_INVENTORY.copy()
    return pd.DataFrame()


@pytest.fixture
def mock_connector():
    conn = MagicMock()
    conn.query_to_df.side_effect = _mock_query_router
    return conn


@pytest.fixture
def optimizer(mock_connector):
    return WarehouseOptimizer(mock_connector)


class TestWorkloadClassification:
    def test_bi_warehouse(self, optimizer):
        assert optimizer.classify_workload("BI_ANALYTICS_WH") == "BI"

    def test_etl_warehouse(self, optimizer):
        assert optimizer.classify_workload("ETL_PIPELINE_WH") == "ETL"

    def test_adhoc_warehouse(self, optimizer):
        assert optimizer.classify_workload("ADHOC_ANALYST_WH") == "AD_HOC"

    def test_ds_warehouse(self, optimizer):
        assert optimizer.classify_workload("DS_TRAINING_WH") == "DS"

    def test_unknown_warehouse(self, optimizer):
        assert optimizer.classify_workload("PROD_GENERAL_WH") == "UNKNOWN"

    def test_case_insensitive(self, optimizer):
        assert optimizer.classify_workload("bi_reporting_wh") == "BI"


class TestAutoSuspendSavings:
    def test_savings_when_suspend_too_high(self, optimizer):
        result = optimizer._calc_auto_suspend_savings(
            "TEST_WH", "MEDIUM", 600, 100.0, "AD_HOC"
        )
        assert result["savings_credits_annual"] > 0

    def test_no_savings_when_already_optimal(self, optimizer):
        result = optimizer._calc_auto_suspend_savings(
            "TEST_WH", "MEDIUM", 60, 100.0, "AD_HOC"
        )
        assert result["savings_credits_annual"] == 0.0

    def test_recommended_suspend_correct_per_workload(self, optimizer):
        expected = {"BI": 300, "ETL": 120, "AD_HOC": 60, "DS": 600}
        for workload, expected_suspend in expected.items():
            result = optimizer._calc_auto_suspend_savings(
                f"{workload}_WH", "MEDIUM", 3600, 100.0, workload
            )
            assert result["recommended_auto_suspend"] == expected_suspend

    def test_detail_string_not_empty(self, optimizer):
        result = optimizer._calc_auto_suspend_savings(
            "TEST_WH", "LARGE", 600, 100.0, "BI"
        )
        assert len(result["detail"]) > 0


class TestRightSizingSavings:
    def test_over_provisioned_generates_savings(self, optimizer):
        result = optimizer._calc_right_sizing_savings(
            "ADHOC_WH", "X-LARGE", 100.0, avg_credits_per_hour=0.5
        )
        assert result["savings_credits_annual"] > 0
        assert result["recommended_size"] != "X-LARGE"

    def test_well_utilized_no_savings(self, optimizer):
        result = optimizer._calc_right_sizing_savings(
            "ETL_WH", "MEDIUM", 200.0, avg_credits_per_hour=3.0
        )
        assert result["savings_credits_annual"] == 0.0
        assert result["recommended_size"] == "MEDIUM"

    def test_minimum_size_no_downsize(self, optimizer):
        result = optimizer._calc_right_sizing_savings(
            "SMALL_WH", "X-SMALL", 5.0, avg_credits_per_hour=0.1
        )
        assert result["savings_credits_annual"] == 0.0

    def test_recommended_one_tier_down(self, optimizer):
        result = optimizer._calc_right_sizing_savings(
            "BI_WH", "LARGE", 320.0, avg_credits_per_hour=0.5
        )
        if result["savings_credits_annual"] > 0:
            assert SIZE_ORDER.index(result["recommended_size"]) == SIZE_ORDER.index("LARGE") - 1

    def test_detail_has_utilization(self, optimizer):
        result = optimizer._calc_right_sizing_savings(
            "TEST_WH", "LARGE", 100.0, avg_credits_per_hour=0.5
        )
        assert "tilization" in result["detail"]


class TestMultiClusterWaste:
    def test_min_cluster_2_generates_savings(self, optimizer):
        result = optimizer._calc_multicluster_waste(
            "BI_WH", min_cluster_count=2, max_cluster_count=4,
            credits_28d=320.0, current_size="LARGE",
        )
        assert result["savings_credits_annual"] > 0

    def test_min_cluster_1_no_waste(self, optimizer):
        result = optimizer._calc_multicluster_waste(
            "ETL_WH", min_cluster_count=1, max_cluster_count=1,
            credits_28d=180.0, current_size="X-LARGE",
        )
        assert result["savings_credits_annual"] == 0.0

    def test_min_cluster_3_more_waste_than_2(self, optimizer):
        result_2 = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=2, max_cluster_count=4,
            credits_28d=400.0, current_size="LARGE",
        )
        result_3 = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=3, max_cluster_count=4,
            credits_28d=400.0, current_size="LARGE",
        )
        assert result_3["savings_credits_annual"] > result_2["savings_credits_annual"]

    def test_detail_mentions_min_cluster(self, optimizer):
        result = optimizer._calc_multicluster_waste(
            "WH", min_cluster_count=2, max_cluster_count=3,
            credits_28d=200.0, current_size="MEDIUM",
        )
        assert "min_cluster_count" in result["detail"]


class TestGetAllRecommendations:
    def test_returns_list(self, optimizer):
        recs = optimizer.get_all_recommendations()
        assert isinstance(recs, list)

    def test_count_matches_mock_data(self, optimizer):
        recs = optimizer.get_all_recommendations()
        assert len(recs) == 4

    def test_required_keys_present(self, optimizer):
        required = [
            "warehouse_name", "workload_type", "current_size",
            "recommended_size", "current_auto_suspend", "recommended_auto_suspend",
            "annual_savings_credits", "savings_calculation_detail",
            "issues", "alter_sql",
        ]
        for rec in optimizer.get_all_recommendations():
            for key in required:
                assert key in rec, f"Missing key '{key}' for {rec.get('warehouse_name')}"

    def test_no_hardcoded_size(self, optimizer):
        # Critical: current_size must come from data, not hardcoded
        recs    = optimizer.get_all_recommendations()
        bi_rec  = next(r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH")
        assert bi_rec["current_size"] == "LARGE"

    def test_etl_size_xlarge(self, optimizer):
        recs    = optimizer.get_all_recommendations()
        etl_rec = next(r for r in recs if r["warehouse_name"] == "ETL_PIPELINE_WH")
        assert etl_rec["current_size"] == "X-LARGE"

    def test_savings_non_negative(self, optimizer):
        for rec in optimizer.get_all_recommendations():
            assert rec["annual_savings_credits"] >= 0.0

    def test_bi_workload_classified_correctly(self, optimizer):
        recs   = optimizer.get_all_recommendations()
        bi_rec = next(r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH")
        assert bi_rec["workload_type"] == "BI"

    def test_alter_sql_is_list(self, optimizer):
        for rec in optimizer.get_all_recommendations():
            assert isinstance(rec["alter_sql"], list)
            assert len(rec["alter_sql"]) > 0

    def test_multi_cluster_warehouse_has_issues(self, optimizer):
        recs   = optimizer.get_all_recommendations()
        bi_rec = next(r for r in recs if r["warehouse_name"] == "BI_ANALYTICS_WH")
        issues_text = " ".join(bi_rec["issues"]).lower()
        assert "cluster" in issues_text