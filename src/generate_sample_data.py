"""
Sample Data Generator — Creates realistic 12-warehouse, 28-day FinOps demo data.
Uploads to FINOPS_DEMO.FINOPS_SAMPLE schema in Snowflake.
Patterns: spike, creep, idle_heavy, multi_cluster_waste, normal.
Author: Shailesh Chalke — Senior Snowflake Consultant

Usage:
    python src/generate_sample_data.py
"""

import os
import sys
import random
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
from snowflake_connector import SnowflakeConnector

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# WAREHOUSE CATALOG (12 warehouses with distinct patterns)
# ─────────────────────────────────────────────
WAREHOUSES = [
    # (name, size, workload_type, auto_suspend, min_cluster, max_cluster, pattern)
    ("BI_ANALYTICS_WH",      "LARGE",   "BI",     600, 2, 4, "normal"),
    ("BI_REPORTING_WH",      "MEDIUM",  "BI",     600, 1, 2, "normal"),
    ("ETL_PIPELINE_WH",      "X-LARGE", "ETL",    600, 1, 1, "spike"),
    ("ETL_DBT_WH",           "LARGE",   "ETL",    600, 1, 1, "normal"),
    ("ADHOC_ANALYST_WH",     "MEDIUM",  "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("ADHOC_SANDBOX_WH",     "MEDIUM",  "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("DS_NOTEBOOKS_WH",      "X-LARGE", "DS",     600, 1, 2, "creep"),
    ("DS_TRAINING_WH",       "2X-LARGE","DS",     600, 2, 4, "multi_cluster_waste"),
    ("ETL_INGEST_WH",        "LARGE",   "ETL",    600, 1, 1, "normal"),
    ("BI_TABLEAU_WH",        "LARGE",   "BI",     600, 2, 3, "multi_cluster_waste"),
    ("ADHOC_EXPLORE_WH",     "SMALL",   "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("DS_FEATURE_ENG_WH",    "X-LARGE", "DS",     600, 1, 1, "spike"),
]

SIZE_BASE_CREDITS = {
    "X-SMALL": 0.5, "SMALL": 1.0, "MEDIUM": 3.0, "LARGE": 8.0,
    "X-LARGE": 18.0, "2X-LARGE": 40.0, "3X-LARGE": 80.0, "4X-LARGE": 160.0,
}

random.seed(42)
np.random.seed(42)


def _generate_daily_credits(
    size: str,
    pattern: str,
    day_index: int,
    num_days: int,
    min_cluster: int,
) -> Tuple[float, float, float, float]:
    """
    Generate (total_credits, idle_credits, cloud_services_credits, query_count)
    for one warehouse-day combination.
    """
    base = SIZE_BASE_CREDITS.get(size, 3.0) * min_cluster

    if pattern == "normal":
        # Weekday/weekend variation
        noise          = np.random.normal(0, base * 0.1)
        weekday_factor = 0.6 if day_index % 7 >= 5 else 1.0
        total          = max(0.5, base * weekday_factor + noise)
        idle           = total * np.random.uniform(0.05, 0.15)

    elif pattern == "spike":
        # Two random spike days in 28-day window
        spike_days = {7, 19}
        if day_index in spike_days:
            total = base * np.random.uniform(4.0, 6.0)  # 4-6x spike
            idle  = total * 0.02
        else:
            total = base * np.random.uniform(0.8, 1.2)
            idle  = total * np.random.uniform(0.05, 0.12)

    elif pattern == "creep":
        # Linear increase: starts at 50%, ends at 150% of base
        growth_factor = 0.5 + (day_index / num_days)
        total         = base * growth_factor * np.random.uniform(0.9, 1.1)
        idle          = total * 0.08

    elif pattern == "idle_heavy":
        # Lots of idle time — high auto-suspend waste
        total = base * np.random.uniform(0.3, 0.5)
        idle  = total * np.random.uniform(0.4, 0.6)   # 40-60% idle

    elif pattern == "multi_cluster_waste":
        # Always-on clusters waste credits at low utilization
        base_multi = base * np.random.uniform(1.0, 1.5)
        total      = base_multi
        idle       = base_multi * np.random.uniform(0.30, 0.50)  # large idle

    else:
        total = base * np.random.uniform(0.8, 1.2)
        idle  = total * 0.10

    cloud_services = total * np.random.uniform(0.03, 0.08)
    query_count    = int(np.random.poisson(lam=max(1, total * 5)))

    return (
        round(float(total), 4),
        round(float(idle), 4),
        round(float(cloud_services), 4),
        query_count,
    )


def generate_metering_rows(days: int = 28) -> List[tuple]:
    """
    Generate metering rows for all 12 warehouses over N days.
    Returns list of tuples for bulk insert.
    """
    rows = []
    today = date.today()

    for wh_name, size, workload, auto_suspend, min_c, max_c, pattern in WAREHOUSES:
        for d in range(days):
            usage_date = today - timedelta(days=days - d - 1)
            total, idle, cloud, queries = _generate_daily_credits(
                size, pattern, d, days, min_c
            )
            rows.append((
                usage_date.isoformat(),  # usage_date
                wh_name,                 # warehouse_name
                size,                    # warehouse_size
                workload,                # workload_type
                auto_suspend,            # auto_suspend
                min_c,                   # min_cluster_count
                max_c,                   # max_cluster_count
                total,                   # total_credits
                idle,                    # idle_credits
                cloud,                   # cloud_services_credits
                queries,                 # query_count
            ))
    return rows


def generate_user_attribution_rows(days: int = 28) -> List[tuple]:
    """
    Generate user-level attribution rows for 8 sample users.
    """
    users = [
        ("alice@hospital.com",  "BI_ANALYTICS_WH"),
        ("bob@hospital.com",    "ETL_PIPELINE_WH"),
        ("carol@hospital.com",  "DS_NOTEBOOKS_WH"),
        ("david@hospital.com",  "ADHOC_ANALYST_WH"),
        ("emily@hospital.com",  "BI_REPORTING_WH"),
        ("frank@hospital.com",  "ETL_DBT_WH"),
        ("grace@hospital.com",  "DS_TRAINING_WH"),
        ("henry@hospital.com",  "ADHOC_EXPLORE_WH"),
    ]
    rows = []
    today = date.today()

    for user_name, wh_name in users:
        for d in range(days):
            usage_date = today - timedelta(days=days - d - 1)
            credits    = round(np.random.exponential(scale=2.0), 4)
            queries    = int(np.random.poisson(lam=15))
            rows.append((
                usage_date.isoformat(),
                user_name,
                wh_name,
                credits,
                queries,
            ))
    return rows


def setup_schema(conn: SnowflakeConnector):
    """Create database, schema, and tables."""
    ddl_statements = [
        "CREATE DATABASE IF NOT EXISTS FINOPS_DEMO",
        "CREATE SCHEMA IF NOT EXISTS FINOPS_DEMO.FINOPS_SAMPLE",

        """
        CREATE OR REPLACE TABLE FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY (
            usage_date              DATE,
            warehouse_name          VARCHAR(100),
            warehouse_size          VARCHAR(20),
            workload_type           VARCHAR(20),
            auto_suspend            INTEGER,
            min_cluster_count       INTEGER,
            max_cluster_count       INTEGER,
            total_credits           FLOAT,
            idle_credits            FLOAT,
            cloud_services_credits  FLOAT,
            query_count             INTEGER
        )
        """,

        """
        CREATE OR REPLACE TABLE FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION (
            usage_date     DATE,
            user_name      VARCHAR(100),
            warehouse_name VARCHAR(100),
            total_credits  FLOAT,
            query_count    INTEGER
        )
        """,
    ]

    for stmt in ddl_statements:
        conn.execute_ddl(stmt.strip())
        logger.info(f"DDL: {stmt.strip()[:60]}…")


def upload_data(conn: SnowflakeConnector):
    """Upload all generated sample data to Snowflake."""

    # Metering data
    logger.info("Generating metering rows…")
    metering_rows = generate_metering_rows(days=28)
    logger.info(f"Uploading {len(metering_rows)} metering rows…")
    conn.execute_many(
        """
        INSERT INTO FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        metering_rows,
    )
    logger.info("✅ Metering data uploaded.")

    # User attribution data
    logger.info("Generating user attribution rows…")
    user_rows = generate_user_attribution_rows(days=28)
    logger.info(f"Uploading {len(user_rows)} user attribution rows…")
    conn.execute_many(
        """
        INSERT INTO FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION
        VALUES (%s, %s, %s, %s, %s)
        """,
        user_rows,
    )
    logger.info("✅ User attribution data uploaded.")


def main():
    logger.info("=" * 60)
    logger.info("Snowflake FinOps Toolkit — Sample Data Generator")
    logger.info("=" * 60)

    conn = SnowflakeConnector()

    logger.info("Step 1: Testing connection…")
    if not conn.test_connection():
        logger.error("Connection failed. Check .env file.")
        sys.exit(1)
    logger.info("✅ Connected to Snowflake.")

    logger.info("Step 2: Creating schema and tables…")
    setup_schema(conn)
    logger.info("✅ Schema ready.")

    logger.info("Step 3: Uploading sample data…")
    upload_data(conn)
    logger.info("✅ Sample data ready.")

    logger.info("")
    logger.info("🎉 Sample data generation complete!")
    logger.info("   Database: FINOPS_DEMO")
    logger.info("   Schema:   FINOPS_SAMPLE")
    logger.info("   Tables:   WH_METERING_HISTORY, USER_ATTRIBUTION")
    logger.info(f"   Warehouses: {len(WAREHOUSES)}")
    logger.info("   Days: 28")

    conn.close()


if __name__ == "__main__":
    main()