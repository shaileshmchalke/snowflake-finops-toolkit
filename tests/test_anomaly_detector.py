"""
Unit tests for AnomalyDetector.
Mock connector used — no live Snowflake connection needed.
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from anomaly_detector import AnomalyDetector


def _build_mock_daily_data() -> pd.DataFrame:
    """Build 28-day mock data with known spike on day 14 for ETL_WH."""
    np.random.seed(42)
    rows = []
    end  = date.today()

    for day_idx in range(28):
        usage_date = end - timedelta(days=(27 - day_idx))

        # BI_WH — normal pattern
        rows.append({
            "usage_date":     usage_date,
            "warehouse_name": "BI_WH",
            "total_credits":  round(float(np.random.normal(10, 1)), 2),
        })

        # ETL_WH — spike on day 14
        if day_idx == 14:
            credits = 80.0
        else:
            credits = round(float(np.random.normal(10, 1)), 2)
        rows.append({
            "usage_date":     usage_date,
            "warehouse_name": "ETL_WH",
            "total_credits":  max(credits, 0.1),
        })

        # DS_WH — creep pattern (monotonically increasing)
        rows.append({
            "usage_date":     usage_date,
            "warehouse_name": "DS_WH",
            "total_credits":  round(5.0 + day_idx * 0.5, 2),
        })

    return pd.DataFrame(rows)


def _mock_query_router(sql: str, params=None) -> pd.DataFrame:
    if "SNOWFLAKE.ACCOUNT_USAGE" in sql and "SELECT 1" in sql:
        raise Exception("Simulated: ACCOUNT_USAGE not available")
    if "WH_METERING_HISTORY" in sql and "usage_date" in sql:
        return _build_mock_daily_data()
    return pd.DataFrame()


@pytest.fixture
def mock_connector():
    conn = MagicMock()
    conn.query_to_df.side_effect = _mock_query_router
    return conn


@pytest.fixture
def detector(mock_connector):
    return AnomalyDetector(mock_connector)


class TestModeDetection:
    def test_falls_back_to_sample(self, detector):
        assert detector._mode == "sample"


class TestTimeseriesWithZscore:
    def test_returns_dataframe(self, detector):
        result = detector.get_timeseries_with_zscore(days=28)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, detector):
        result = detector.get_timeseries_with_zscore(days=28)
        assert "warehouse_name"  in result.columns
        assert "usage_date"      in result.columns
        assert "total_credits"   in result.columns
        assert "z_score"         in result.columns

    def test_not_empty(self, detector):
        result = detector.get_timeseries_with_zscore(days=28)
        assert not result.empty

    def test_zscore_no_nan(self, detector):
        result = detector.get_timeseries_with_zscore(days=28)
        assert not result["z_score"].isna().any()

    def test_flat_warehouse_zscore_is_zero(self):
        """FIX test: std=0 warehouse must have z_score=0, not error."""
        conn = MagicMock()

        def flat_data(sql, params=None):
            if "SELECT 1" in sql:
                raise Exception("no ACCOUNT_USAGE")
            rows = []
            for i in range(28):
                rows.append({
                    "usage_date":     date.today() - timedelta(days=i),
                    "warehouse_name": "FLAT_WH",
                    "total_credits":  5.0,  # constant — std=0
                })
            return pd.DataFrame(rows)

        conn.query_to_df.side_effect = flat_data
        det    = AnomalyDetector(conn)
        result = det.get_timeseries_with_zscore(days=28)
        assert (result["z_score"] == 0.0).all()


class TestDetectSpikes:
    def test_returns_list(self, detector):
        result = detector.detect_spikes()
        assert isinstance(result, list)

    def test_detects_etl_spike(self, detector):
        spikes = detector.detect_spikes()
        spike_warehouses = [s["warehouse_name"] for s in spikes]
        assert "ETL_WH" in spike_warehouses

    def test_spike_has_required_keys(self, detector):
        spikes = detector.detect_spikes()
        if spikes:
            for key in ["warehouse_name", "usage_date", "total_credits", "z_score"]:
                assert key in spikes[0]

    def test_spike_zscore_above_threshold(self, detector):
        spikes = detector.detect_spikes()
        for spike in spikes:
            assert abs(spike["z_score"]) >= 3.0


class TestDetectSlowCreep:
    def test_returns_list(self, detector):
        result = detector.detect_slow_creep()
        assert isinstance(result, list)

    def test_detects_ds_wh_creep(self, detector):
        creep = detector.detect_slow_creep()
        # DS_WH has 28 days of increasing values — should be detected
        wh_names = [c["warehouse_name"] for c in creep]
        assert "DS_WH" in wh_names

    def test_creep_has_required_keys(self, detector):
        creep = detector.detect_slow_creep()
        if creep:
            for key in ["warehouse_name", "consecutive_days", "total_increase_pct"]:
                assert key in creep[0]

    def test_creep_consecutive_days_positive(self, detector):
        creep = detector.detect_slow_creep()
        for c in creep:
            assert c["consecutive_days"] > 0