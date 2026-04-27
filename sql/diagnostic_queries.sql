-- =============================================================================
-- Snowflake FinOps Toolkit — Diagnostic Queries
-- Author: Shailesh Chalke — Senior Snowflake Consultant
-- Description: 8 production diagnostic queries for cost optimization
-- Usage: Run against SNOWFLAKE.ACCOUNT_USAGE or FINOPS_DEMO.FINOPS_SAMPLE
-- =============================================================================


-- =============================================================================
-- QUERY 1: Idle Waste Detection
-- Identifies warehouses billing compute credits with low/zero query activity.
-- Idle time = warehouse running but not executing queries.
-- =============================================================================
SELECT
    warehouse_name,
    DATE_TRUNC('DAY', start_time)::DATE                          AS billing_date,
    SUM(credits_used_compute)                                    AS compute_credits,
    SUM(credits_used_cloud_services)                             AS cloud_svc_credits,
    SUM(credits_used)                                            AS total_credits,
    -- Idle ratio: high cloud_svc vs compute suggests idle spinning
    ROUND(SUM(credits_used_cloud_services)
          / NULLIF(SUM(credits_used_compute), 0) * 100, 2)      AS cloud_svc_pct,
    -- Flag warehouses where cloud services > 10% of compute
    CASE
        WHEN SUM(credits_used_cloud_services)
             / NULLIF(SUM(credits_used_compute), 0) > 0.10
        THEN 'INVESTIGATE'
        ELSE 'NORMAL'
    END                                                           AS status
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('DAY', -30, CURRENT_DATE())
  AND start_time <  CURRENT_TIMESTAMP()
GROUP BY 1, 2
HAVING SUM(credits_used) > 0
ORDER BY compute_credits DESC
LIMIT 50;


-- =============================================================================
-- QUERY 2: Warehouse Utilization — Credits per Executing Minute
-- Low credits_per_active_minute = warehouse oversized for workload.
-- =============================================================================
WITH warehouse_activity AS (
    SELECT
        wh.warehouse_name,
        SUM(wh.credits_used)                        AS total_credits_28d,
        COUNT(DISTINCT qh.query_id)                 AS total_queries,
        SUM(qh.total_elapsed_time) / 60000.0        AS total_exec_minutes,
        -- Credits per active minute = efficiency metric
        ROUND(SUM(wh.credits_used)
              / NULLIF(SUM(qh.total_elapsed_time) / 60000.0, 0), 4) AS credits_per_exec_min
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wh
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON wh.warehouse_name = qh.warehouse_name
        AND DATE_TRUNC('HOUR', wh.start_time) = DATE_TRUNC('HOUR', qh.start_time)
    WHERE wh.start_time >= DATEADD('DAY', -28, CURRENT_DATE())
    GROUP BY 1
)
SELECT
    warehouse_name,
    ROUND(total_credits_28d, 2)      AS total_credits_28d,
    total_queries,
    ROUND(total_exec_minutes, 1)     AS active_exec_minutes,
    credits_per_exec_min,
    -- Recommendation flag
    CASE
        WHEN credits_per_exec_min IS NULL OR total_queries = 0
            THEN 'NO_ACTIVITY — consider suspending'
        WHEN credits_per_exec_min < 0.01
            THEN 'OVERSIZED — downsize recommended'
        WHEN credits_per_exec_min > 1.0
            THEN 'HIGH_UTILIZATION — monitor for spill'
        ELSE 'NORMAL'
    END AS recommendation
FROM warehouse_activity
ORDER BY total_credits_28d DESC;


-- =============================================================================
-- QUERY 3: Cloud Services Cost Monitoring
-- Cloud services > 10% of total = investigate compilation overhead.
-- Snowflake bills cloud services separately; excess = optimization opportunity.
-- =============================================================================
SELECT
    DATE_TRUNC('DAY', start_time)::DATE              AS billing_date,
    SUM(credits_used_compute)                         AS compute_credits,
    SUM(credits_used_cloud_services)                  AS cloud_svc_credits,
    SUM(credits_used)                                 AS total_credits,
    ROUND(
        SUM(credits_used_cloud_services)
        / NULLIF(SUM(credits_used_compute), 0) * 100, 2
    )                                                 AS cloud_svc_pct_of_compute,
    -- Daily USD estimate at $3/credit
    ROUND(SUM(credits_used) * 3.00, 2)               AS estimated_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('DAY', -30, CURRENT_DATE())
  AND start_time <  CURRENT_TIMESTAMP()
GROUP BY 1
ORDER BY 1 DESC;


-- =============================================================================
-- QUERY 4: Multi-Cluster Warehouse Waste Detection
-- Multi-cluster with min_cluster > 1 = always-on credits even at zero load.
-- Most warehouses need min_cluster = 1 (Economy scaling policy).
-- =============================================================================
SELECT
    w.name                                          AS warehouse_name,
    w.size                                          AS warehouse_size,
    w.min_cluster_count,
    w.max_cluster_count,
    w.auto_suspend,
    w.scaling_policy,
    -- Credits wasted by always-on extra clusters
    (w.min_cluster_count - 1)                       AS always_on_extra_clusters,
    -- Look up recent usage
    ROUND(SUM(h.credits_used), 2)                   AS credits_last_28d
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES w
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY h
    ON w.name = h.warehouse_name
    AND h.start_time >= DATEADD('DAY', -28, CURRENT_DATE())
WHERE w.deleted IS NULL
  AND w.min_cluster_count > 1
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY always_on_extra_clusters DESC, credits_last_28d DESC;


-- =============================================================================
-- QUERY 5: Result Cache Hit Rate
-- Low cache hit rate for BI warehouses = lost optimization opportunity.
-- Target: > 40% cache hit for BI workloads.
-- =============================================================================
SELECT
    warehouse_name,
    COUNT(*)                                                     AS total_queries,
    SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
    -- Cache hits: query reused result set
    SUM(CASE
            WHEN bytes_scanned = 0
             AND compilation_time = 0
             AND execution_time < 100
            THEN 1
            ELSE 0
        END)                                                     AS cache_hit_queries,
    ROUND(
        SUM(CASE
                WHEN bytes_scanned = 0
                 AND compilation_time = 0
                 AND execution_time < 100
                THEN 1
                ELSE 0
            END) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                            AS cache_hit_rate_pct,
    -- Partition pruning effectiveness
    ROUND(AVG(CASE
                  WHEN partitions_total > 0
                  THEN (1 - partitions_scanned::FLOAT / partitions_total) * 100
                  ELSE 0
              END), 2)                                           AS avg_partition_prune_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('DAY', -28, CURRENT_DATE())
  AND warehouse_name IS NOT NULL
  AND warehouse_name != ''
GROUP BY 1
HAVING COUNT(*) >= 10
ORDER BY total_queries DESC
LIMIT 30;


-- =============================================================================
-- QUERY 6: Per-User Credit Attribution (MTD)
-- Identify top credit consumers for chargeback / showback reporting.
-- =============================================================================
SELECT
    user_name,
    warehouse_name,
    COUNT(DISTINCT query_id)                    AS query_count,
    ROUND(SUM(credits_used_cloud_services), 4)  AS cloud_svc_credits,
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)  AS avg_query_sec,
    ROUND(MAX(total_elapsed_time) / 1000.0, 2)  AS max_query_sec,
    -- Estimated credits: proportional allocation by execution time
    ROUND(
        SUM(total_elapsed_time)::FLOAT
        / NULLIF(
            SUM(SUM(total_elapsed_time)) OVER (PARTITION BY warehouse_name), 0
          ) * 100, 2
    )                                           AS pct_of_warehouse_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATE_TRUNC('MONTH', CURRENT_DATE())
  AND warehouse_name IS NOT NULL
  AND user_name IS NOT NULL
GROUP BY 1, 2
ORDER BY cloud_svc_credits DESC
LIMIT 30;


-- =============================================================================
-- QUERY 7: Top 50 Most Expensive Queries (Last 7 Days)
-- Identifies long-running queries that should be optimized or killed.
-- High bytes_spilled_to_remote_disk = warehouse too small for this query.
-- =============================================================================
SELECT
    query_id,
    user_name,
    warehouse_name,
    warehouse_size,
    query_type,
    -- Execution times
    ROUND(total_elapsed_time / 1000.0, 2)            AS elapsed_sec,
    ROUND(compilation_time   / 1000.0, 2)            AS compile_sec,
    ROUND(execution_time     / 1000.0, 2)            AS execute_sec,
    -- Data volume
    ROUND(bytes_scanned      / 1073741824.0, 3)      AS gb_scanned,
    ROUND(bytes_spilled_to_local_storage / 1073741824.0, 3)  AS gb_spilled_local,
    ROUND(bytes_spilled_to_remote_storage / 1073741824.0, 3) AS gb_spilled_remote,
    -- Efficiency flags
    CASE
        WHEN bytes_spilled_to_remote_storage > 0
        THEN 'UPSIZE_WAREHOUSE — remote spill detected'
        WHEN bytes_spilled_to_local_storage > 1073741824  -- > 1 GB local spill
        THEN 'CONSIDER_UPSIZE — heavy local spill'
        WHEN total_elapsed_time > 300000  -- > 5 minutes
        THEN 'OPTIMIZE_QUERY — long running'
        ELSE 'NORMAL'
    END                                               AS recommendation,
    LEFT(query_text, 200)                             AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('DAY', -7, CURRENT_DATE())
  AND execution_status = 'SUCCESS'
  AND total_elapsed_time > 10000  -- > 10 seconds
ORDER BY total_elapsed_time DESC
LIMIT 50;


-- =============================================================================
-- QUERY 8: Auto-Suspend Impact Analysis
-- Compare credits consumed before and after auto-suspend optimization.
-- Use this to validate savings after applying ALTER WAREHOUSE changes.
-- =============================================================================
WITH hourly_billing AS (
    SELECT
        warehouse_name,
        DATE_TRUNC('HOUR', start_time)::TIMESTAMP  AS billing_hour,
        SUM(credits_used)                           AS hour_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('DAY', -56, CURRENT_DATE())  -- last 8 weeks
    GROUP BY 1, 2
),
with_query_counts AS (
    SELECT
        h.warehouse_name,
        h.billing_hour,
        h.hour_credits,
        COUNT(q.query_id) AS queries_in_hour,
        -- Hours with zero queries = pure idle billing
        CASE WHEN COUNT(q.query_id) = 0 THEN 1 ELSE 0 END AS is_idle_hour
    FROM hourly_billing h
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
        ON h.warehouse_name = q.warehouse_name
        AND DATE_TRUNC('HOUR', q.start_time) = h.billing_hour
    GROUP BY 1, 2, 3
)
SELECT
    warehouse_name,
    COUNT(*)                                          AS total_billed_hours,
    SUM(is_idle_hour)                                 AS idle_billed_hours,
    ROUND(SUM(is_idle_hour) * 100.0 / COUNT(*), 2)   AS idle_pct,
    ROUND(SUM(CASE WHEN is_idle_hour = 1
                   THEN hour_credits ELSE 0 END), 2)  AS idle_credits_wasted,
    ROUND(SUM(hour_credits), 2)                       AS total_credits,
    -- Projected annual idle waste
    ROUND(
        SUM(CASE WHEN is_idle_hour = 1
                 THEN hour_credits ELSE 0 END)
        * (365.0 / 56), 2
    )                                                 AS projected_annual_idle_credits,
    -- USD at $3/credit
    ROUND(
        SUM(CASE WHEN is_idle_hour = 1
                 THEN hour_credits ELSE 0 END)
        * (365.0 / 56) * 3.00, 2
    )                                                 AS projected_annual_idle_usd
FROM with_query_counts
GROUP BY 1
HAVING SUM(is_idle_hour) > 0
ORDER BY idle_credits_wasted DESC;