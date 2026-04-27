"""
Unit tests for CostAnalyzer.
Uses mock connector — no live Snowflake connection needed.
Author: Shailesh Chalke — Senior Snowflake Consultant

Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cost_analyzer import CostAnalyzer


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────
@pytest.fixture
def mock_connector():
    """Create a mock SnowflakeConnector for testing."""
    conn = MagicMock()
    # Default: simulate sample data mode (ACCOUNT_USAGE unavailable)
    conn.query_to_df.side_effect = _mock_query_router
    return conn


def _mock_query_router(sql: str, params=None) -> pd.DataFrame:
    """Route mock SQL queries to appropriate fake data."""

    # Detect mode check
    if "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY" in sql and "SELECT 1" in sql:
        raise Exception("Simulated: ACCOUNT_USAGE not available")

    # MTD cost
    if "SUM(total_credits)" in sql and "DATE_TRUNC('MONTH'" in sql and "USER_ATTRIBUTION" not in sql:
        return pd.DataFrame({"total_credits": [450.0]})

    # Daily trend
    if "usage_date" in sql and "SUM(total_credits)" in sql and "GROUP BY 1, 2" in sql:
        dates = pd.date_range("2024-01-01", periods=28)
        rows = []
        import numpy as np
        np.random.seed(42)
        for d in dates:
            for wh in ["BI_WH", "ETL_WH", "ADHOC_WH"]:
                rows.append({
                    "usage_date":    d,
                    "warehouse_name": wh,
                    "total_credits": round(float(np.random.exponential(10)), 2),
                })
        return pd.DataFrame(rows)

    # User attribution
    if "USER_ATTRIBUTION" in sql:
        return pd.DataFrame({
            "user_name":     ["alice@h.com", "bob@h.com", "carol@h.com"],
            "total_credits": [120.5, 85.3, 42.1],
            "query_count":   [450, 320, 180],
        })

    # Cloud services
    if "cloud_services_credits" in sql.lower():
        return pd.DataFrame({"cloud_credits": [28.5]})

    # Idle waste
    if "idle_credits" in sql.lower():
        return pd.DataFrame({"idle_credits": [65.0]})

    # YTD
    if "DATE_TRUNC('YEAR'" in sql:
        return pd.DataFrame({"total_credits": [3200.0]})

    return pd.DataFrame({"total_credits": [0.0]})


# ─────────────────────────────────────────────
# TEST: MODE DETECTION
# ─────────────────────────────────────────────
def test_mode_detection_falls_back_to_sample(mock_connector):
    """CostAnalyzer should fall back to 'sample' mode when ACCOUNT_USAGE is unavailable."""
    analyzer = CostAnalyzer(mock_connector)
    assert analyzer._mode == "sample"


# ─────────────────────────────────────────────
# TEST: CREDIT → USD CONVERSION
# ─────────────────────────────────────────────
def test_credits_to_usd_default_price():
    """100 credits × $3.00 = $300.00"""
    result = CostAnalyzer.credits_to_usd(100.0)
    assert result == 300.00


def test_credits_to_usd_custom_price():
    """50 credits × $2.50 = $125.00"""
    result = CostAnalyzer.credits_to_usd(50.0, price_per_credit=2.50)
    assert result == 125.00


def test_credits_to_usd_zero():
    """0 credits = $0.00"""
    result = CostAnalyzer.credits_to_usd(0.0)
    assert result == 0.00


def test_credits_to_usd_large():
    """10000 credits × $3.00 = $30000.00"""
    result = CostAnalyzer.credits_to_usd(10000.0)
    assert result == 30000.00


# ─────────────────────────────────────────────
# TEST: MTD COST
# ─────────────────────────────────────────────
def test_get_mtd_cost_returns_float(mock_connector):
    """MTD cost should return a positive float."""
    analyzer = CostAnalyzer(mock_connector)
    mtd = analyzer.get_mtd_cost()
    assert isinstance(mtd, float)
    assert mtd >= 0.0


def test_get_mtd_cost_value(mock_connector):
    """MTD cost should return 450.0 credits from mock data."""
    analyzer = CostAnalyzer(mock_connector)
    mtd = analyzer.get_mtd_cost()
    assert mtd == 450.0


# ─────────────────────────────────────────────
# TEST: DAILY TREND
# ─────────────────────────────────────────────
def test_get_daily_cost_trend_shape(mock_connector):
    """Daily trend should return a DataFrame with required columns."""
    analyzer = CostAnalyzer(mock_connector)
    df = analyzer.get_daily_cost_trend(days=28)
    assert isinstance(df, pd.DataFrame)
    assert "usage_date"    in df.columns
    assert "total_credits" in df.columns


def test_get_daily_cost_trend_non_empty(mock_connector):
    """Daily trend DataFrame should not be empty."""
    analyzer = CostAnalyzer(mock_connector)
    df = analyzer.get_daily_cost_trend(days=28)
    assert len(df) > 0


def test_get_daily_cost_trend_credits_positive(mock_connector):
    """All credit values in the trend should be >= 0."""
    analyzer = CostAnalyzer(mock_connector)
    df = analyzer.get_daily_cost_trend(days=28)
    assert (df["total_credits"] >= 0).all()


# ─────────────────────────────────────────────
# TEST: USER ATTRIBUTION
# ─────────────────────────────────────────────
def test_get_user_attribution_columns(mock_connector):
    """User attribution should have user_name and total_credits columns."""
    analyzer = CostAnalyzer(mock_connector)
    df = analyzer.get_user_attribution()
    assert "user_name"     in df.columns
    assert "total_credits" in df.columns


def test_get_user_attribution_row_count(mock_connector):
    """User attribution should return 3 rows from mock data."""
    analyzer = CostAnalyzer(mock_connector)
    df = analyzer.get_user_attribution()
    assert len(df) == 3


# ─────────────────────────────────────────────
# TEST: CLOUD SERVICES
# ─────────────────────────────────────────────
def test_get_cloud_services_cost_positive(mock_connector):
    """Cloud services cost should be a positive float."""
    analyzer = CostAnalyzer(mock_connector)
    cloud = analyzer.get_cloud_services_cost()
    assert isinstance(cloud, float)
    assert cloud >= 0.0


# ─────────────────────────────────────────────
# TEST: IDLE WASTE
# ─────────────────────────────────────────────
def test_get_idle_waste_positive(mock_connector):
    """Idle waste should be a positive float."""
    analyzer = CostAnalyzer(mock_connector)
    idle = analyzer.get_idle_waste()
    assert isinstance(idle, float)
    assert idle >= 0.0


# ─────────────────────────────────────────────
# TEST: ERROR HANDLING — connector failure
# ─────────────────────────────────────────────
def test_mtd_cost_handles_empty_df():
    """CostAnalyzer should return 0.0 if query returns empty DataFrame."""
    conn = MagicMock()

    # Mode detection fails → sample mode
    def side_effect(sql, params=None):
        if "SELECT 1" in sql:
            raise Exception("No ACCOUNT_USAGE")
        return pd.DataFrame()  # empty for everything else

    conn.query_to_df.side_effect = side_effect
    analyzer = CostAnalyzer(conn)
    assert analyzer.get_mtd_cost() == 0.0


def test_ytd_cost_returns_float(mock_connector):
    """YTD cost should return a float >= 0."""
    analyzer = CostAnalyzer(mock_connector)
    ytd = analyzer.get_ytd_cost()
    assert isinstance(ytd, float)
    assert ytd >= 0.0