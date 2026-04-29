"""
Unit tests for CostAnalyzer.
Mock connector used — no live Snowflake connection needed.
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from cost_analyzer import CostAnalyzer


def _mock_query_router(sql: str, params=None) -> pd.DataFrame:
    if "SNOWFLAKE.ACCOUNT_USAGE" in sql and "SELECT 1" in sql:
        raise Exception("Simulated: ACCOUNT_USAGE not available")

    if "SUM(total_credits)" in sql and "DATE_TRUNC('MONTH'" in sql and "USER_ATTRIBUTION" not in sql:
        return pd.DataFrame({"total_credits": [450.0]})

    if "DATE_TRUNC('YEAR'" in sql:
        return pd.DataFrame({"total_credits": [2400.0]})

    if "usage_date" in sql and "warehouse_name" in sql and "GROUP BY 1, 2" in sql:
        dates = pd.date_range("2024-01-01", periods=28)
        rows  = []
        np.random.seed(42)
        for d in dates:
            for wh in ["BI_WH", "ETL_WH", "ADHOC_WH"]:
                rows.append({
                    "usage_date":     d,
                    "warehouse_name": wh,
                    "total_credits":  round(float(np.random.exponential(10)), 2),
                })
        return pd.DataFrame(rows)

    if "USER_ATTRIBUTION" in sql:
        return pd.DataFrame({
            "user_name":     ["alice@h.com", "bob@h.com", "carol@h.com"],
            "total_credits": [120.5, 85.3, 42.1],
            "query_count":   [450, 320, 180],
        })

    if "cloud_services_credits" in sql or "CLOUD_SERVICES" in sql.upper():
        return pd.DataFrame({"total_credits": [22.5]})

    if "idle_credits" in sql:
        return pd.DataFrame({"idle_credits": [60.0]})

    return pd.DataFrame({"total_credits": [0.0]})


@pytest.fixture
def mock_connector():
    conn = MagicMock()
    conn.query_to_df.side_effect = _mock_query_router
    return conn


@pytest.fixture
def analyzer(mock_connector):
    return CostAnalyzer(mock_connector)


class TestModeDetection:
    def test_falls_back_to_sample_mode(self, analyzer):
        assert analyzer._mode == "sample"


class TestMtdCost:
    def test_returns_float(self, analyzer):
        result = analyzer.get_mtd_cost()
        assert isinstance(result, float)

    def test_positive_value(self, analyzer):
        result = analyzer.get_mtd_cost()
        assert result > 0

    def test_mtd_value(self, analyzer):
        result = analyzer.get_mtd_cost()
        assert result == 450.0


class TestYtdCost:
    def test_returns_float(self, analyzer):
        result = analyzer.get_ytd_cost()
        assert isinstance(result, float)

    def test_ytd_greater_than_mtd(self, analyzer):
        mtd = analyzer.get_mtd_cost()
        ytd = analyzer.get_ytd_cost()
        assert ytd >= mtd


class TestDailyTrend:
    def test_returns_dataframe(self, analyzer):
        result = analyzer.get_daily_cost_trend(days=28)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, analyzer):
        result = analyzer.get_daily_cost_trend(days=28)
        assert "usage_date" in result.columns
        assert "warehouse_name" in result.columns
        assert "total_credits" in result.columns

    def test_not_empty(self, analyzer):
        result = analyzer.get_daily_cost_trend(days=28)
        assert not result.empty


class TestUserAttribution:
    def test_returns_dataframe(self, analyzer):
        result = analyzer.get_user_attribution()
        assert isinstance(result, pd.DataFrame)

    def test_has_user_name_column(self, analyzer):
        result = analyzer.get_user_attribution()
        assert "user_name" in result.columns


class TestCreditsToUsd:
    def test_default_price(self):
        result = CostAnalyzer.credits_to_usd(100)
        assert result == 300.0

    def test_custom_price(self):
        result = CostAnalyzer.credits_to_usd(100, price_per_credit=2.00)
        assert result == 200.0

    def test_zero_credits(self):
        result = CostAnalyzer.credits_to_usd(0)
        assert result == 0.0

    def test_fractional_credits(self):
        result = CostAnalyzer.credits_to_usd(1.5, price_per_credit=3.00)
        assert result == 4.50