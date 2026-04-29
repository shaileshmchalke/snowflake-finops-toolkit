"""
Sample Data Generator - Creates 12-warehouse, 28-day FinOps demo data.
Uploads to FINOPS_DEMO.FINOPS_SAMPLE schema in Snowflake.

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

# FIX: warehouse_optimizer.py सोबत consistent values
# आधी SMALL=1.0, MEDIUM=3.0 होते — आता SMALL=2, MEDIUM=4 (Snowflake actual values)
SIZE_BASE_CREDITS = {
    "X-SMALL": 1,
    "SMALL":   2,
    "MEDIUM":  4,
    "LARGE":   8,
    "X-LARGE": 16,
    "2X-LARGE": 32,
    "3X-LARGE": 64,
    "4X-LARGE": 128,
}

WAREHOUSES = [
    # (name, size, workload_type, auto_suspend, min_cluster, max_cluster, pattern)
    ("BI_ANALYTICS_WH",   "LARGE",   "BI",     600, 2, 4, "normal"),
    ("BI_REPORTING_WH",   "MEDIUM",  "BI",     600, 1, 2, "normal"),
    ("ETL_PIPELINE_WH",   "X-LARGE", "ETL",    600, 1, 1, "spike"),
    ("ETL_DBT_WH",        "LARGE",   "ETL",    600, 1, 1, "normal"),
    ("ADHOC_ANALYST_WH",  "MEDIUM",  "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("ADHOC_SANDBOX_WH",  "MEDIUM",  "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("DS_NOTEBOOKS_WH",   "X-LARGE", "DS",     600, 1, 2, "creep"),
    ("DS_TRAINING_WH",    "2X-LARGE","DS",     600, 2, 4, "multi_cluster_waste"),
    ("ETL_INGEST_WH",     "LARGE",   "ETL",    600, 1, 1, "normal"),
    ("BI_TABLEAU_WH",     "LARGE",   "BI",     600, 2, 3, "multi_cluster_waste"),
    ("ADHOC_EXPLORE_WH",  "SMALL",   "AD_HOC", 900, 1, 1, "idle_heavy"),
    ("DS_FEATURE_ENG_WH", "X-LARGE", "DS",     600, 1, 1, "spike"),
]

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
    Generate daily credits for a warehouse based on its usage pattern.
    Returns: (total_credits, compute_credits, cloud_services_credits, idle_credits)
    """
    base = SIZE_BASE_CREDITS.get(size, 4) * min_cluster

    if pattern == "normal":
        compute = base * random.uniform(0.6, 1.2)

    elif pattern == "spike":
        # 3 spike days in 28-day window
        spike_days = {5, 14, 22}
        if day_index in spike_days:
            compute = base * random.uniform(4.0, 6.0)
        else:
            compute = base * random.uniform(0.5, 1.0)

    elif pattern == "idle_heavy":
        # 60% of time idle — auto_suspend too high
        if random.random() < 0.6:
            compute = base * random.uniform(0.05, 0.15)
        else:
            compute = base * random.uniform(0.4, 0.8)

    elif pattern == "creep":
        # Gradual increase over 28 days
        growth_factor = 1 + (day_index / num_days) * 0.8
        compute = base * random.uniform(0.5, 0.9) * growth_factor

    elif pattern == "multi_cluster_waste":
        # Always billing for min_cluster even at low load
        compute = base * random.uniform(0.3, 0.7)

    else:
        compute = base * random.uniform(0.5, 1.0)

    compute            = round(max(compute, 0.01), 4)
    cloud_services     = round(compute * random.uniform(0.02, 0.08), 4)
    idle_credits       = round(compute * random.uniform(0.1, 0.4), 4)
    total              = round(compute + cloud_services, 4)

    return total, compute, cloud_services, idle_credits


def create_schema(conn: SnowflakeConnector):
    """Create FINOPS_DEMO database and FINOPS_SAMPLE schema."""
    logger.info("Creating FINOPS_DEMO database and FINOPS_SAMPLE schema...")
    conn.execute_ddl("CREATE DATABASE IF NOT EXISTS FINOPS_DEMO")
    conn.execute_ddl("USE DATABASE FINOPS_DEMO")
    conn.execute_ddl("CREATE SCHEMA IF NOT EXISTS FINOPS_SAMPLE")
    conn.execute_ddl("USE SCHEMA FINOPS_SAMPLE")
    logger.info("Schema ready.")


def create_tables(conn: SnowflakeConnector):
    """Create WH_METERING_HISTORY and USER_ATTRIBUTION tables."""
    logger.info("Creating tables...")

    conn.execute_ddl("""
        CREATE OR REPLACE TABLE FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY (
            usage_date              DATE          NOT NULL,
            warehouse_name          VARCHAR(100)  NOT NULL,
            warehouse_size          VARCHAR(20)   NOT NULL,
            auto_suspend            INTEGER       NOT NULL,
            min_cluster_count       INTEGER       NOT NULL,
            max_cluster_count       INTEGER       NOT NULL,
            total_credits           FLOAT         NOT NULL,
            compute_credits         FLOAT         NOT NULL,
            cloud_services_credits  FLOAT         NOT NULL,
            idle_credits            FLOAT         NOT NULL
        )
    """)

    conn.execute_ddl("""
        CREATE OR REPLACE TABLE FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION (
            usage_date              DATE          NOT NULL,
            user_name               VARCHAR(100)  NOT NULL,
            warehouse_name          VARCHAR(100)  NOT NULL,
            total_credits           FLOAT         NOT NULL,
            query_count             INTEGER       NOT NULL
        )
    """)

    logger.info("Tables created.")


def generate_metering_data() -> List[tuple]:
    """Generate 12 warehouses x 28 days = 336 rows of metering data."""
    num_days = 28
    end_date = date.today()
    rows     = []

    for (name, size, workload, auto_suspend, min_cl, max_cl, pattern) in WAREHOUSES:
        for day_idx in range(num_days):
            usage_date = end_date - timedelta(days=(num_days - 1 - day_idx))
            total, compute, cloud_svc, idle = _generate_daily_credits(
                size, pattern, day_idx, num_days, min_cl
            )
            rows.append((
                usage_date, name, size, auto_suspend,
                min_cl, max_cl, total, compute, cloud_svc, idle
            ))

    return rows


def generate_user_data() -> List[tuple]:
    """Generate 28 days of user attribution data."""
    users = [
        "alice@hospital.com", "bob@hospital.com",
        "carol@hospital.com", "dave@hospital.com",
        "eve@hospital.com",
    ]
    num_days = 28
    end_date = date.today()
    rows     = []

    for day_idx in range(num_days):
        usage_date = end_date - timedelta(days=(num_days - 1 - day_idx))
        for user in users:
            for (wh_name, _, _, _, _, _, _) in WAREHOUSES[:4]:
                if random.random() > 0.4:
                    credits     = round(random.uniform(0.5, 15.0), 4)
                    query_count = random.randint(5, 200)
                    rows.append((usage_date, user, wh_name, credits, query_count))

    return rows


def upload_data(conn: SnowflakeConnector, metering_rows: List[tuple], user_rows: List[tuple]):
    """Upload generated data to Snowflake."""
    logger.info(f"Uploading {len(metering_rows)} metering rows...")
    conn.execute_many(
        """
        INSERT INTO FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY
        (usage_date, warehouse_name, warehouse_size, auto_suspend,
         min_cluster_count, max_cluster_count, total_credits,
         compute_credits, cloud_services_credits, idle_credits)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        metering_rows,
    )
    logger.info(f"Metering data uploaded.")

    logger.info(f"Uploading {len(user_rows)} user attribution rows...")
    conn.execute_many(
        """
        INSERT INTO FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION
        (usage_date, user_name, warehouse_name, total_credits, query_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        user_rows,
    )
    logger.info("User attribution data uploaded.")


def verify_upload(conn: SnowflakeConnector):
    """Verify row counts after upload."""
    metering_df = conn.query_to_df(
        "SELECT COUNT(*) AS cnt FROM FINOPS_DEMO.FINOPS_SAMPLE.WH_METERING_HISTORY"
    )
    user_df = conn.query_to_df(
        "SELECT COUNT(*) AS cnt FROM FINOPS_DEMO.FINOPS_SAMPLE.USER_ATTRIBUTION"
    )
    logger.info(f"WH_METERING_HISTORY rows: {metering_df['cnt'].iloc[0]}")
    logger.info(f"USER_ATTRIBUTION rows:    {user_df['cnt'].iloc[0]}")


def main():
    logger.info("Starting sample data generation...")
    conn = SnowflakeConnector()

    if not conn.test_connection():
        logger.error("Connection test failed. Check .env credentials.")
        sys.exit(1)

    create_schema(conn)
    create_tables(conn)

    metering_rows = generate_metering_data()
    user_rows     = generate_user_data()

    upload_data(conn, metering_rows, user_rows)
    verify_upload(conn)

    conn.close()
    logger.info("Sample data generation complete!")
    logger.info("Run: streamlit run app/streamlit_app.py")


if __name__ == "__main__":
    main()