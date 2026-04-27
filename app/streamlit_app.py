"""
Snowflake FinOps Toolkit — Main Streamlit Dashboard
Author: Shailesh Chalke — Senior Snowflake Consultant
Description: 5-page production-grade cost optimization dashboard
"""

import copy
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cost_analyzer import CostAnalyzer
from warehouse_optimizer import WarehouseOptimizer
from anomaly_detector import AnomalyDetector
from bulk_configurator import BulkConfigurator
from snowflake_connector import SnowflakeConnector

load_dotenv()

# ─────────────────────────────────────────────
# PAGE CONFIG & CUSTOM CSS
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Snowflake FinOps Toolkit",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
    /* Main background */
    .main { background-color: #0e1117; }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1e2130, #252a3a);
        border: 1px solid #3a4060;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        transition: transform 0.2s;
    }
    .kpi-card:hover { transform: translateY(-2px); }
    .kpi-value { font-size: 2.2rem; font-weight: 700; color: #56ccf2; margin: 8px 0; }
    .kpi-label { font-size: 0.85rem; color: #9aa5b4; text-transform: uppercase; letter-spacing: 0.08em; }
    .kpi-delta-pos { font-size: 0.9rem; color: #27ae60; }
    .kpi-delta-neg { font-size: 0.9rem; color: #e74c3c; }

    /* Warehouse Cards */
    .wh-card {
        background: #1a1f2e;
        border-left: 4px solid #56ccf2;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 12px;
    }
    .wh-card-critical { border-left-color: #e74c3c; }
    .wh-card-warning  { border-left-color: #f39c12; }
    .wh-card-good     { border-left-color: #27ae60; }

    /* Section Headers */
    .section-header {
        font-size: 1.4rem;
        font-weight: 600;
        color: #56ccf2;
        border-bottom: 2px solid #3a4060;
        padding-bottom: 8px;
        margin-bottom: 20px;
    }

    /* Anomaly badges */
    .badge-spike  { background:#e74c3c; color:#fff; border-radius:4px; padding:2px 8px; font-size:0.78rem; }
    .badge-creep  { background:#f39c12; color:#fff; border-radius:4px; padding:2px 8px; font-size:0.78rem; }
    .badge-normal { background:#27ae60; color:#fff; border-radius:4px; padding:2px 8px; font-size:0.78rem; }

    /* Sidebar */
    .css-1d391kg { background-color: #13161f; }
    
    /* Buttons */
    .stDownloadButton > button {
        background: linear-gradient(90deg,#56ccf2,#2f80ed);
        color: white; border: none; border-radius: 8px;
        padding: 8px 20px; font-weight: 600;
    }
    
    /* Tables */
    .dataframe { font-size: 0.85rem; }
    
    /* Simulator box */
    .sim-box {
        background: #1e2130;
        border: 1px solid #3a4060;
        border-radius: 10px;
        padding: 20px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# CONNECTION & DATA LOADING
# ─────────────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_connector():
    """Create and cache Snowflake connector (one connection per session)."""
    return SnowflakeConnector()


@st.cache_data(ttl=300, show_spinner="Loading cost data…")
def load_cost_data(_conn):
    """Cache cost data for 5 minutes."""
    analyzer = CostAnalyzer(_conn)
    return {
        "mtd":            analyzer.get_mtd_cost(),
        "ytd":            analyzer.get_ytd_cost(),
        "daily_trend":    analyzer.get_daily_cost_trend(days=28),
        "cloud_services": analyzer.get_cloud_services_cost(),
        "user_attr":      analyzer.get_user_attribution(),
        "idle_waste":     analyzer.get_idle_waste(),
    }


@st.cache_data(ttl=300, show_spinner="Analyzing warehouses…")
def load_warehouse_data(_conn):
    """Cache warehouse optimization data for 5 minutes."""
    optimizer = WarehouseOptimizer(_conn)
    return optimizer.get_all_recommendations()


@st.cache_data(ttl=300, show_spinner="Running anomaly detection…")
def load_anomaly_data(_conn):
    """Cache anomaly detection results for 5 minutes."""
    detector = AnomalyDetector(_conn)
    return {
        "timeseries": detector.get_timeseries_with_zscore(days=28),
        "spikes":     detector.detect_spikes(),
        "creep":      detector.detect_slow_creep(),
    }


@st.cache_data(ttl=600, show_spinner="Loading configurator data…")
def load_configurator_data(_conn):
    """Cache bulk configurator data for 10 minutes."""
    cfg = BulkConfigurator(_conn)
    return cfg.get_grouped_recommendations()


# ─────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────
def render_sidebar(conn):
    with st.sidebar:
        st.markdown("## ❄️ FinOps Toolkit")
        st.markdown("**Senior Snowflake Consultant**")
        st.markdown("*Shailesh Chalke*")
        st.divider()

        page = st.radio(
            "Navigation",
            [
                "📊 Cost Overview",
                "🏭 Warehouse Optimizer",
                "🚨 Anomaly Detection",
                "⚙️ Bulk Configurator",
                "🔮 What-If Simulator",
            ],
            label_visibility="collapsed",
        )
        st.divider()

        credit_price = st.number_input(
            "💰 Credit Price (USD)", min_value=1.0, max_value=10.0,
            value=3.00, step=0.25, help="On-Demand: $3.00 | Enterprise: $2.00+"
        )
        st.session_state["credit_price"] = credit_price

        st.divider()
        if st.button("🔄 Refresh All Data"):
            st.cache_data.clear()
            st.rerun()

        st.markdown(
            "<small style='color:#9aa5b4;'>Data refreshes every 5 min<br>"
            "Last refresh: just now</small>",
            unsafe_allow_html=True,
        )
    return page


# ─────────────────────────────────────────────
# PAGE 1 — COST OVERVIEW
# ─────────────────────────────────────────────
def page_cost_overview(conn, credit_price: float):
    st.markdown('<div class="section-header">📊 Cost Overview — MTD / YTD / Trends</div>',
                unsafe_allow_html=True)

    data = load_cost_data(conn)

    mtd        = data["mtd"]
    ytd        = data["ytd"]
    idle_waste = data["idle_waste"]
    cloud_svc  = data["cloud_services"]

    mtd_usd   = mtd  * credit_price
    ytd_usd   = ytd  * credit_price
    idle_usd  = idle_waste * credit_price
    cloud_usd = cloud_svc  * credit_price

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    kpi_data = [
        (col1, "MTD Spend",          f"${mtd_usd:,.0f}",   f"{mtd:,.1f} credits",    "kpi-delta-neg"),
        (col2, "YTD Spend",          f"${ytd_usd:,.0f}",   f"{ytd:,.1f} credits",    "kpi-delta-neg"),
        (col3, "Idle Waste MTD",     f"${idle_usd:,.0f}",  "Recoverable savings",    "kpi-delta-neg"),
        (col4, "Cloud Services MTD", f"${cloud_usd:,.0f}", ">10% = action needed",   "kpi-delta-pos"),
    ]
    for col, label, value, sub, delta_class in kpi_data:
        with col:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-label">{label}</div>'
                f'<div class="kpi-value">{value}</div>'
                f'<div class="{delta_class}">{sub}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # Daily Cost Trend — Area Chart
    daily_df = data["daily_trend"]
    if not daily_df.empty:
        daily_df = daily_df.copy()
        daily_df["usd_cost"] = daily_df["total_credits"] * credit_price

        fig_trend = px.area(
            daily_df, x="usage_date", y="usd_cost",
            title="28-Day Daily Cost Trend (USD)",
            color_discrete_sequence=["#56ccf2"],
            labels={"usage_date": "Date", "usd_cost": "Cost (USD)"},
        )
        fig_trend.update_layout(
            plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
            font_color="#c8d6e5", title_font_size=16,
            hovermode="x unified",
        )
        fig_trend.update_xaxes(gridcolor="#2a3040")
        fig_trend.update_yaxes(gridcolor="#2a3040")
        st.plotly_chart(fig_trend, use_container_width=True)

    col_left, col_right = st.columns(2)

    # Warehouse Cost Breakdown
    with col_left:
        if not daily_df.empty and "warehouse_name" in daily_df.columns:
            wh_summary = (
                daily_df.groupby("warehouse_name")["total_credits"]
                .sum()
                .reset_index()
                .sort_values("total_credits", ascending=False)
                .head(10)
            )
            fig_bar = px.bar(
                wh_summary, x="total_credits", y="warehouse_name",
                orientation="h", title="Top 10 Warehouses by Credits (28d)",
                color="total_credits",
                color_continuous_scale=["#1a1f2e", "#56ccf2"],
                labels={"total_credits": "Credits", "warehouse_name": "Warehouse"},
            )
            fig_bar.update_layout(
                plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
                font_color="#c8d6e5", showlegend=False,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Warehouse breakdown not available in aggregated mode.")

    # User Attribution
    with col_right:
        user_df = data["user_attr"]
        if not user_df.empty:
            user_df = user_df.copy()
            user_df["usd"] = user_df["total_credits"] * credit_price
            fig_pie = px.pie(
                user_df.head(8), values="usd", names="user_name",
                title="Credit Attribution by User (MTD)",
                color_discrete_sequence=px.colors.sequential.Blues_r,
                hole=0.45,
            )
            fig_pie.update_layout(
                plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
                font_color="#c8d6e5",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    # Cloud Services Warning
    if cloud_svc > 0:
        ratio = cloud_usd / max(mtd_usd, 1) * 100
        if ratio > 10:
            st.error(
                f"⚠️ Cloud Services = {ratio:.1f}% of total spend (>${cloud_usd:,.0f}). "
                "This exceeds Snowflake's 10% guideline. Review query compilation overhead."
            )
        else:
            st.success(
                f"✅ Cloud Services = {ratio:.1f}% of total spend — within healthy range."
            )


# ─────────────────────────────────────────────
# PAGE 2 — WAREHOUSE OPTIMIZER
# ─────────────────────────────────────────────
def page_warehouse_optimizer(conn, credit_price: float):
    st.markdown('<div class="section-header">🏭 Warehouse Optimizer — Per-Warehouse Savings</div>',
                unsafe_allow_html=True)

    recommendations = load_warehouse_data(conn)

    if not recommendations:
        st.warning("No warehouse data available. Generate sample data first.")
        return

    total_annual_savings = sum(r.get("annual_savings_credits", 0) for r in recommendations)
    total_annual_usd     = total_annual_savings * credit_price
    flagged_count        = sum(1 for r in recommendations if r.get("annual_savings_credits", 0) > 0)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Warehouses Analyzed", len(recommendations))
    with col2:
        st.metric("Warehouses with Savings", flagged_count)
    with col3:
        st.metric("Total Annual Savings", f"${total_annual_usd:,.0f}")

    st.markdown("---")

    # Filter
    workload_types = list({r.get("workload_type", "UNKNOWN") for r in recommendations})
    selected_workloads = st.multiselect(
        "Filter by Workload Type", workload_types, default=workload_types
    )

    filtered = [r for r in recommendations if r.get("workload_type") in selected_workloads]
    filtered.sort(key=lambda x: x.get("annual_savings_credits", 0), reverse=True)

    for rec in filtered:
        savings_credits = rec.get("annual_savings_credits", 0)
        savings_usd     = savings_credits * credit_price

        if savings_usd > 20000:
            card_class = "wh-card wh-card-critical"
        elif savings_usd > 5000:
            card_class = "wh-card wh-card-warning"
        else:
            card_class = "wh-card wh-card-good"

        with st.container():
            st.markdown(
                f'<div class="{card_class}">'
                f'<b style="color:#56ccf2;font-size:1.1rem;">{rec["warehouse_name"]}</b> '
                f'&nbsp;|&nbsp; <span style="color:#9aa5b4;">Type: {rec.get("workload_type","UNKNOWN")}</span>'
                f'&nbsp;|&nbsp; <span style="color:#9aa5b4;">Current Size: {rec.get("current_size","UNKNOWN")}</span>'
                f'&nbsp;|&nbsp; <span style="color:#f39c12;font-weight:600;">💰 ${savings_usd:,.0f}/yr</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

            with st.expander(f"🔍 View Savings Calculations — {rec['warehouse_name']}"):
                c1, c2, c3 = st.columns(3)
                c1.metric("Current Auto-Suspend",
                          f"{rec.get('current_auto_suspend', 0)}s",
                          delta=f"Recommended: {rec.get('recommended_auto_suspend', 0)}s",
                          delta_color="inverse")
                c2.metric("Current Size",
                          rec.get("current_size", "UNKNOWN"),
                          delta=f"Recommended: {rec.get('recommended_size', rec.get('current_size'))}",
                          delta_color="off")
                c3.metric("Annual Savings",
                          f"${savings_usd:,.0f}",
                          delta=f"{savings_credits:,.1f} credits",
                          delta_color="normal")

                st.markdown("**📐 Savings Calculation Detail:**")
                st.code(rec.get("savings_calculation_detail", "N/A"), language="text")

                # Issues list
                issues = rec.get("issues", [])
                if issues:
                    st.markdown("**⚠️ Issues Detected:**")
                    for issue in issues:
                        st.markdown(f"- {issue}")

                # SQL suggestion
                sql_cmds = rec.get("alter_sql", [])
                if sql_cmds:
                    st.markdown("**🛠️ Recommended SQL:**")
                    for sql in sql_cmds:
                        st.code(sql, language="sql")


# ─────────────────────────────────────────────
# PAGE 3 — ANOMALY DETECTION
# ─────────────────────────────────────────────
def page_anomaly_detection(conn, credit_price: float):
    st.markdown('<div class="section-header">🚨 Anomaly Detection — Z-Score & Trend Analysis</div>',
                unsafe_allow_html=True)

    data = load_anomaly_data(conn)
    ts_df  = data["timeseries"]
    spikes = data["spikes"]
    creep  = data["creep"]

    if ts_df.empty:
        st.warning("No timeseries data available.")
        return

    spike_count  = len(spikes)
    creep_count  = len(creep)
    max_z        = ts_df["z_score"].max() if "z_score" in ts_df.columns else 0
    total_anomaly_usd = (
        ts_df.loc[ts_df["z_score"].abs() > 3.0, "total_credits"].sum() * credit_price
        if "z_score" in ts_df.columns else 0
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Spike Anomalies (z>3)", spike_count)
    col2.metric("Slow Creep Detections", creep_count)
    col3.metric("Max Z-Score (28d)", f"{max_z:.2f}")
    col4.metric("Anomalous Spend", f"${total_anomaly_usd:,.0f}")

    st.markdown("---")

    # Dual-axis chart: credits + z-score
    ts_df = ts_df.copy()
    ts_df["usd_cost"] = ts_df["total_credits"] * credit_price

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=ts_df["usage_date"], y=ts_df["usd_cost"],
            name="Daily Cost (USD)", fill="tozeroy",
            line=dict(color="#56ccf2", width=2),
            fillcolor="rgba(86,204,242,0.15)",
        ),
        secondary_y=False,
    )

    if "z_score" in ts_df.columns:
        # Color z-score line by severity
        colors = ts_df["z_score"].apply(
            lambda z: "#e74c3c" if abs(z) > 3 else ("#f39c12" if abs(z) > 2 else "#27ae60")
        )
        fig.add_trace(
            go.Scatter(
                x=ts_df["usage_date"], y=ts_df["z_score"],
                name="Z-Score", mode="lines+markers",
                line=dict(color="#f39c12", width=2, dash="dot"),
                marker=dict(
                    color=colors,
                    size=ts_df["z_score"].abs().apply(lambda z: 8 if z > 3 else 5),
                ),
            ),
            secondary_y=True,
        )
        # Z-score threshold lines
        fig.add_hline(y=3.0,  line_dash="dash", line_color="#e74c3c",
                      annotation_text="z=+3 (spike)", secondary_y=True)
        fig.add_hline(y=-3.0, line_dash="dash", line_color="#e74c3c",
                      annotation_text="z=-3", secondary_y=True)

    fig.update_layout(
        title="28-Day Cost Trend with Z-Score Anomaly Detection",
        plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
        font_color="#c8d6e5", hovermode="x unified", height=450,
    )
    fig.update_xaxes(gridcolor="#2a3040")
    fig.update_yaxes(title_text="Cost (USD)", gridcolor="#2a3040", secondary_y=False)
    fig.update_yaxes(title_text="Z-Score",    gridcolor="#2a3040", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # Tabs: Spike | Creep
    tab_spike, tab_creep = st.tabs(["🔴 Spike Anomalies", "🟡 Slow Creep"])

    with tab_spike:
        if spikes:
            spike_df = pd.DataFrame(spikes)
            spike_df["cost_usd"] = spike_df["total_credits"] * credit_price
            st.dataframe(
                spike_df[["warehouse_name", "usage_date", "total_credits", "cost_usd", "z_score"]]
                .sort_values("z_score", ascending=False)
                .reset_index(drop=True)
                .style.background_gradient(subset=["z_score"], cmap="Reds"),
                use_container_width=True,
            )
            st.info(
                f"🔴 **{len(spikes)} spike events** detected (z-score > 3.0). "
                "Review these dates for runaway queries or unexpected workloads."
            )
        else:
            st.success("✅ No spike anomalies detected in the last 28 days.")

    with tab_creep:
        if creep:
            creep_df = pd.DataFrame(creep)
            st.dataframe(creep_df, use_container_width=True)
            st.warning(
                f"🟡 **{len(creep)} slow-creep patterns** detected. "
                "These warehouses show 7+ consecutive days of cost increases — "
                "likely query regression or data volume growth."
            )
        else:
            st.success("✅ No slow-creep patterns detected in the last 28 days.")


# ─────────────────────────────────────────────
# PAGE 4 — BULK CONFIGURATOR
# ─────────────────────────────────────────────
def page_bulk_configurator(conn, credit_price: float):
    st.markdown('<div class="section-header">⚙️ Bulk Configurator — Grouped SQL Generation</div>',
                unsafe_allow_html=True)

    grouped = load_configurator_data(conn)

    if not grouped:
        st.warning("No configuration data available.")
        return

    all_alter_sql    = []
    all_rollback_sql = []

    st.markdown(
        "**Instructions:** Review grouped SQL commands by workload type. "
        "Download the rollback script before applying any changes in production."
    )

    for workload_type, group_data in grouped.items():
        warehouses  = group_data.get("warehouses", [])
        alter_sqls  = group_data.get("alter_sqls", [])
        rollback_sqls = group_data.get("rollback_sqls", [])
        total_savings = group_data.get("total_annual_savings_usd", 0) * credit_price

        all_alter_sql.extend(alter_sqls)
        all_rollback_sql.extend(rollback_sqls)

        color_map = {
            "BI":      "#56ccf2",
            "ETL":     "#f39c12",
            "AD_HOC":  "#9b59b6",
            "DS":      "#27ae60",
            "UNKNOWN": "#95a5a6",
        }
        color = color_map.get(workload_type, "#56ccf2")

        with st.expander(
            f"**{workload_type}** Workload — {len(warehouses)} warehouse(s) — "
            f"💰 ${total_savings:,.0f}/yr potential savings",
            expanded=False,
        ):
            st.markdown(f"**Warehouses:** {', '.join(warehouses)}")
            st.markdown(f"**Recommended Auto-Suspend:**")
            suspend_map = {"BI": "300s (5 min)", "ETL": "120s (2 min)",
                           "AD_HOC": "60s (1 min)", "DS": "600s (10 min)"}
            st.info(f"⏱️ {suspend_map.get(workload_type, '120s (2 min)')}")

            if alter_sqls:
                st.markdown("**🛠️ ALTER SQL Commands:**")
                st.code("\n\n".join(alter_sqls), language="sql")

            if rollback_sqls:
                st.markdown("**↩️ Rollback SQL Commands:**")
                st.code("\n\n".join(rollback_sqls), language="sql")

    # Download buttons
    st.markdown("---")
    col1, col2 = st.columns(2)

    if all_alter_sql:
        alter_content = (
            "-- Snowflake FinOps Toolkit — Bulk ALTER Script\n"
            "-- Generated by: Shailesh Chalke — Senior Snowflake Consultant\n"
            "-- WARNING: Review each command before executing in PRODUCTION\n\n"
            + "\n\n".join(all_alter_sql)
        )
        col1.download_button(
            label="⬇️ Download ALTER Script",
            data=alter_content,
            file_name="finops_bulk_alter.sql",
            mime="text/plain",
        )

    if all_rollback_sql:
        rollback_content = (
            "-- Snowflake FinOps Toolkit — ROLLBACK Script\n"
            "-- Run this IMMEDIATELY if issues arise after ALTER\n\n"
            + "\n\n".join(all_rollback_sql)
        )
        col2.download_button(
            label="⬇️ Download Rollback Script",
            data=rollback_content,
            file_name="finops_rollback.sql",
            mime="text/plain",
        )

    st.warning(
        "⚠️ **Production Checklist:** "
        "(1) Download rollback script first. "
        "(2) Apply during low-traffic window. "
        "(3) Monitor for 48 hours after changes. "
        "(4) Validate with business stakeholders."
    )


# ─────────────────────────────────────────────
# PAGE 5 — WHAT-IF SIMULATOR
# ─────────────────────────────────────────────
def page_whatif_simulator(conn, credit_price: float):
    st.markdown('<div class="section-header">🔮 What-If Simulator — Cost Scenario Modeling</div>',
                unsafe_allow_html=True)

    st.markdown(
        "Adjust the parameters below to model cost savings scenarios. "
        "All calculations are live — no page reload needed."
    )
    st.info(
        "💡 **BI Warehouse Cache Tip:** BI warehouses use result caching. "
        "Reducing auto-suspend below 300s may **invalidate the result cache** "
        "and increase query compilation costs. Verify cache hit rate before reducing."
    )

    # ── Input Parameters ──────────────────────
    st.markdown("### 🎛️ Scenario Parameters")

    col1, col2, col3 = st.columns(3)

    with col1:
        num_warehouses = st.slider(
            "Number of Warehouses", min_value=1, max_value=50, value=12
        )
        avg_credits_per_day = st.slider(
            "Avg Credits/Warehouse/Day", min_value=1.0, max_value=200.0,
            value=24.0, step=1.0
        )

    with col2:
        current_auto_suspend_s = st.slider(
            "Current Auto-Suspend (seconds)", min_value=60, max_value=3600,
            value=600, step=60
        )
        new_auto_suspend_s = st.slider(
            "New Auto-Suspend (seconds)", min_value=60, max_value=3600,
            value=120, step=60
        )

    with col3:
        idle_fraction_pct = st.slider(
            "Idle Time % (current)", min_value=0, max_value=100, value=35
        )
        right_size_reduction_pct = st.slider(
            "Right-Sizing Reduction %", min_value=0, max_value=80, value=25
        )

    bi_warehouse_pct = st.slider(
        "% of Warehouses that are BI (cache-sensitive)", min_value=0, max_value=100, value=30
    )

    # ── Deep copy scenario state (prevents Streamlit mutation bug) ──
    base_scenario = {
        "num_warehouses":          num_warehouses,
        "avg_credits_per_day":     avg_credits_per_day,
        "current_auto_suspend_s":  current_auto_suspend_s,
        "new_auto_suspend_s":      new_auto_suspend_s,
        "idle_fraction_pct":       idle_fraction_pct,
        "right_size_reduction_pct": right_size_reduction_pct,
        "credit_price":            credit_price,
        "bi_warehouse_pct":        bi_warehouse_pct,
    }
    scenario = copy.copy(base_scenario)

    # ── LIVE CALCULATION ──────────────────────
    annual_base_credits = (
        scenario["num_warehouses"]
        * scenario["avg_credits_per_day"]
        * 365
    )

    # 1. Auto-suspend savings
    suspend_reduction_ratio = max(
        0.0,
        (scenario["current_auto_suspend_s"] - scenario["new_auto_suspend_s"])
        / max(scenario["current_auto_suspend_s"], 1),
    )
    idle_saving_fraction = (scenario["idle_fraction_pct"] / 100) * suspend_reduction_ratio
    auto_suspend_savings_credits = annual_base_credits * idle_saving_fraction

    # 2. Right-sizing savings
    right_size_savings_credits = (
        annual_base_credits * (scenario["right_size_reduction_pct"] / 100)
    )

    # 3. BI cache warning adjustment
    bi_cache_risk_credits = 0.0
    bi_warehouses = round(scenario["num_warehouses"] * scenario["bi_warehouse_pct"] / 100)
    if scenario["new_auto_suspend_s"] < 300 and bi_warehouses > 0:
        # Cache loss adds ~5% overhead per BI warehouse
        bi_cache_risk_credits = (
            bi_warehouses * scenario["avg_credits_per_day"] * 365 * 0.05
        )

    total_savings_credits = (
        auto_suspend_savings_credits + right_size_savings_credits - bi_cache_risk_credits
    )
    total_savings_usd = total_savings_credits * scenario["credit_price"]
    annual_base_usd   = annual_base_credits   * scenario["credit_price"]
    savings_pct       = (total_savings_usd / max(annual_base_usd, 1)) * 100

    # ── Results Display ───────────────────────
    st.markdown("---")
    st.markdown("### 📊 Simulation Results")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Annual Baseline Cost", f"${annual_base_usd:,.0f}",
              f"{annual_base_credits:,.0f} credits")
    r2.metric("Auto-Suspend Savings", f"${auto_suspend_savings_credits * credit_price:,.0f}",
              f"{auto_suspend_savings_credits:,.0f} credits")
    r3.metric("Right-Sizing Savings", f"${right_size_savings_credits * credit_price:,.0f}",
              f"{right_size_savings_credits:,.0f} credits")
    r4.metric("Net Annual Savings", f"${total_savings_usd:,.0f}",
              f"{savings_pct:.1f}% reduction")

    if bi_cache_risk_credits > 0:
        st.warning(
            f"⚠️ **BI Cache Risk Penalty:** ${bi_cache_risk_credits * credit_price:,.0f}/yr added "
            f"due to {bi_warehouses} BI warehouse(s) with auto-suspend < 300s. "
            "This estimate assumes 5% cache-miss overhead."
        )

    # ── Formula Breakdown ─────────────────────
    st.markdown("### 📐 Formula Breakdown")
    with st.container():
        st.markdown('<div class="sim-box">', unsafe_allow_html=True)
        st.code(
            f"""ANNUAL BASELINE
  = {num_warehouses} warehouses × {avg_credits_per_day} credits/day × 365 days
  = {annual_base_credits:,.0f} credits  (${annual_base_usd:,.0f})

AUTO-SUSPEND SAVINGS
  Suspend reduction ratio = ({current_auto_suspend_s}s - {new_auto_suspend_s}s) / {current_auto_suspend_s}s
                          = {suspend_reduction_ratio:.3f}  ({suspend_reduction_ratio*100:.1f}%)
  Idle saving fraction    = {idle_fraction_pct}% idle × {suspend_reduction_ratio*100:.1f}% reduction
                          = {idle_saving_fraction*100:.2f}%
  Savings                 = {annual_base_credits:,.0f} × {idle_saving_fraction:.4f}
                          = {auto_suspend_savings_credits:,.0f} credits  (${auto_suspend_savings_credits*credit_price:,.0f})

RIGHT-SIZING SAVINGS
  = {annual_base_credits:,.0f} × {right_size_reduction_pct}%
  = {right_size_savings_credits:,.0f} credits  (${right_size_savings_credits*credit_price:,.0f})

BI CACHE RISK PENALTY
  BI warehouses with suspend < 300s: {bi_warehouses}
  Penalty                           = {bi_warehouses} × {avg_credits_per_day} × 365 × 5%
                                    = {bi_cache_risk_credits:,.0f} credits  (${bi_cache_risk_credits*credit_price:,.0f})

NET SAVINGS
  = {auto_suspend_savings_credits:,.0f} + {right_size_savings_credits:,.0f} - {bi_cache_risk_credits:,.0f}
  = {total_savings_credits:,.0f} credits  (${total_savings_usd:,.0f})
  = {savings_pct:.1f}% reduction from baseline
""",
            language="text",
        )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Visual ────────────────────────────────
    chart_data = pd.DataFrame({
        "Category": ["Baseline Cost", "Auto-Suspend Savings",
                      "Right-Sizing Savings", "BI Cache Penalty", "Net Cost"],
        "Amount USD": [
            annual_base_usd,
            auto_suspend_savings_credits * credit_price,
            right_size_savings_credits   * credit_price,
            bi_cache_risk_credits        * credit_price,
            annual_base_usd - total_savings_usd,
        ],
        "Type": ["baseline", "saving", "saving", "penalty", "net"],
    })
    color_map = {
        "baseline": "#56ccf2", "saving": "#27ae60",
        "penalty": "#e74c3c", "net": "#f39c12"
    }
    fig = px.bar(
        chart_data, x="Category", y="Amount USD",
        color="Type", color_discrete_map=color_map,
        title="What-If Scenario: Annual Cost Breakdown",
    )
    fig.update_layout(
        plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
        font_color="#c8d6e5", showlegend=True,
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    try:
        conn = get_connector()
    except Exception as e:
        st.error(
            f"❄️ **Snowflake Connection Failed:** {e}\n\n"
            "Please check your `.env` file and run `make setup-sample-data` first."
        )
        st.stop()

    credit_price = st.session_state.get("credit_price", 3.00)
    page = render_sidebar(conn)

    if page == "📊 Cost Overview":
        page_cost_overview(conn, credit_price)
    elif page == "🏭 Warehouse Optimizer":
        page_warehouse_optimizer(conn, credit_price)
    elif page == "🚨 Anomaly Detection":
        page_anomaly_detection(conn, credit_price)
    elif page == "⚙️ Bulk Configurator":
        page_bulk_configurator(conn, credit_price)
    elif page == "🔮 What-If Simulator":
        page_whatif_simulator(conn, credit_price)


if __name__ == "__main__":
    main()