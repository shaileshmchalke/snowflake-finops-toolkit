"""
Snowflake FinOps Toolkit - Main Dashboard
Author: Shailesh Chalke
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

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cost_analyzer import CostAnalyzer
from warehouse_optimizer import WarehouseOptimizer
from anomaly_detector import AnomalyDetector
from bulk_configurator import BulkConfigurator
from snowflake_connector import SnowflakeConnector

load_dotenv()

st.set_page_config(
    page_title="Snowflake FinOps Toolkit",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
    .main { background-color: #0e1117; }

    .kpi-card {
        background: linear-gradient(135deg, #1e2130, #252a3a);
        border: 1px solid #3a4060;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .kpi-value { font-size: 2.2rem; font-weight: 700; color: #56ccf2; margin: 8px 0; }
    .kpi-label { font-size: 0.85rem; color: #9aa5b4; text-transform: uppercase; letter-spacing: 0.08em; }
    .kpi-delta-pos { font-size: 0.9rem; color: #27ae60; }
    .kpi-delta-neg { font-size: 0.9rem; color: #e74c3c; }

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

    .section-header {
        font-size: 1.4rem;
        font-weight: 600;
        color: #56ccf2;
        border-bottom: 2px solid #3a4060;
        padding-bottom: 8px;
        margin-bottom: 20px;
    }

    .sim-box {
        background: #1e2130;
        border: 1px solid #3a4060;
        border-radius: 10px;
        padding: 20px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


@st.cache_resource(show_spinner="Connecting to Snowflake...")
def get_connector():
    return SnowflakeConnector()


@st.cache_data(ttl=300, show_spinner="Loading cost data...")
def load_cost_data(_conn):
    analyzer = CostAnalyzer(_conn)
    return {
        "mtd":            analyzer.get_mtd_cost(),
        "ytd":            analyzer.get_ytd_cost(),
        "daily_trend":    analyzer.get_daily_cost_trend(days=28),
        "cloud_services": analyzer.get_cloud_services_cost(),
        "user_attr":      analyzer.get_user_attribution(),
        "idle_waste":     analyzer.get_idle_waste(),
    }


@st.cache_data(ttl=300, show_spinner="Analyzing warehouses...")
def load_warehouse_data(_conn):
    optimizer = WarehouseOptimizer(_conn)
    return optimizer.get_all_recommendations()


@st.cache_data(ttl=300, show_spinner="Running anomaly detection...")
def load_anomaly_data(_conn):
    detector = AnomalyDetector(_conn)
    return {
        "timeseries": detector.get_timeseries_with_zscore(days=28),
        "spikes":     detector.detect_spikes(),
        "creep":      detector.detect_slow_creep(),
    }


@st.cache_data(ttl=600, show_spinner="Loading configurator data...")
def load_configurator_data(_conn, credit_price: float):
    # FIX: credit_price parameter pass केला — hardcoded $3.00 नाही
    cfg = BulkConfigurator(_conn)
    return cfg.get_grouped_recommendations(credit_price=credit_price)


def render_sidebar(conn):
    with st.sidebar:
        st.markdown("## ❄️ FinOps Toolkit")
        st.markdown("**Shailesh Chalke**")
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
            "💰 Credit Price (USD)",
            min_value=1.0,
            max_value=10.0,
            value=3.00,
            step=0.25,
            help="On-Demand: $3.00 | Enterprise: $2.00+",
        )
        st.session_state["credit_price"] = credit_price

        st.divider()
        if st.button("🔄 Refresh All Data"):
            st.cache_data.clear()
            st.rerun()

        st.markdown(
            "<small style='color:#9aa5b4;'>Data refreshes every 5 min</small>",
            unsafe_allow_html=True,
        )

    # FIX: page आणि credit_price दोन्ही return करतो
    return page, credit_price


def page_cost_overview(conn, credit_price: float):
    st.markdown(
        '<div class="section-header">📊 Cost Overview — MTD / YTD / Trends</div>',
        unsafe_allow_html=True,
    )

    data = load_cost_data(conn)

    mtd        = data["mtd"]
    ytd        = data["ytd"]
    idle_waste = data["idle_waste"]
    cloud_svc  = data["cloud_services"]

    mtd_usd   = mtd        * credit_price
    ytd_usd   = ytd        * credit_price
    idle_usd  = idle_waste * credit_price
    cloud_usd = cloud_svc  * credit_price

    col1, col2, col3, col4 = st.columns(4)
    kpi_items = [
        (col1, "MTD Spend",          f"${mtd_usd:,.0f}",   f"{mtd:,.1f} credits",     "kpi-delta-neg"),
        (col2, "YTD Spend",          f"${ytd_usd:,.0f}",   f"{ytd:,.1f} credits",     "kpi-delta-neg"),
        (col3, "Idle Waste MTD",     f"${idle_usd:,.0f}",  "Recoverable savings",     "kpi-delta-neg"),
        (col4, "Cloud Services MTD", f"${cloud_usd:,.0f}", ">10% = action needed",    "kpi-delta-pos"),
    ]
    for col, label, value, sub, delta_cls in kpi_items:
        with col:
            st.markdown(
                f'<div class="kpi-card">'
                f'<div class="kpi-label">{label}</div>'
                f'<div class="kpi-value">{value}</div>'
                f'<div class="{delta_cls}">{sub}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    daily_df = data["daily_trend"]
    if not daily_df.empty:
        plot_df = daily_df.copy()
        plot_df["usd_cost"] = plot_df["total_credits"] * credit_price

        fig_trend = px.area(
            plot_df, x="usage_date", y="usd_cost",
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
            st.info("💡 Warehouse breakdown available only with ACCOUNT_USAGE access.")

    with col_right:
        user_df = data["user_attr"]
        if not user_df.empty:
            plot_user = user_df.copy()
            plot_user["usd"] = plot_user["total_credits"] * credit_price
            fig_pie = px.pie(
                plot_user.head(8), values="usd", names="user_name",
                title="Credit Attribution by User (MTD)",
                color_discrete_sequence=px.colors.sequential.Blues_r,
                hole=0.45,
            )
            fig_pie.update_layout(
                plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
                font_color="#c8d6e5",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    if cloud_svc > 0 and mtd_usd > 0:
        ratio = cloud_usd / mtd_usd * 100
        if ratio > 10:
            st.error(
                f"⚠️ Cloud Services = {ratio:.1f}% of total spend (${cloud_usd:,.0f}). "
                "Exceeds Snowflake's 10% guideline. Review query compilation overhead."
            )
        else:
            st.success(f"✅ Cloud Services = {ratio:.1f}% of total spend — within healthy range.")


def page_warehouse_optimizer(conn, credit_price: float):
    st.markdown(
        '<div class="section-header">🏭 Warehouse Optimizer — Per-Warehouse Savings</div>',
        unsafe_allow_html=True,
    )

    recommendations = load_warehouse_data(conn)

    if not recommendations:
        st.warning("No warehouse data found. Run: python src/generate_sample_data.py")
        return

    total_annual_savings = sum(r.get("annual_savings_credits", 0) for r in recommendations)
    total_annual_usd     = total_annual_savings * credit_price
    flagged_count        = sum(1 for r in recommendations if r.get("annual_savings_credits", 0) > 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Warehouses Analyzed", len(recommendations))
    col2.metric("Warehouses with Savings",   flagged_count)
    col3.metric("Total Annual Savings",       f"${total_annual_usd:,.0f}")

    st.markdown("---")

    workload_types = sorted({r.get("workload_type", "UNKNOWN") for r in recommendations})
    selected = st.multiselect("Filter by Workload Type", workload_types, default=workload_types)

    filtered = [r for r in recommendations if r.get("workload_type") in selected]
    filtered.sort(key=lambda x: x.get("annual_savings_credits", 0), reverse=True)

    for rec in filtered:
        savings_credits = rec.get("annual_savings_credits", 0)
        savings_usd     = savings_credits * credit_price

        if savings_usd > 20000:
            card_cls = "wh-card wh-card-critical"
        elif savings_usd > 5000:
            card_cls = "wh-card wh-card-warning"
        else:
            card_cls = "wh-card wh-card-good"

        st.markdown(
            f'<div class="{card_cls}">'
            f'<b style="color:#56ccf2;font-size:1.1rem;">{rec["warehouse_name"]}</b>'
            f'&nbsp;|&nbsp;<span style="color:#9aa5b4;">Type: {rec.get("workload_type","UNKNOWN")}</span>'
            f'&nbsp;|&nbsp;<span style="color:#9aa5b4;">Size: {rec.get("current_size","UNKNOWN")}</span>'
            f'&nbsp;|&nbsp;<span style="color:#f39c12;font-weight:600;">💰 ${savings_usd:,.0f}/yr</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        with st.expander(f"🔍 Savings Detail — {rec['warehouse_name']}"):
            c1, c2, c3 = st.columns(3)
            c1.metric(
                "Current Auto-Suspend",
                f"{rec.get('current_auto_suspend', 0)}s",
                delta=f"Recommended: {rec.get('recommended_auto_suspend', 0)}s",
                delta_color="inverse",
            )
            c2.metric(
                "Current Size",
                rec.get("current_size", "UNKNOWN"),
                delta=f"Recommended: {rec.get('recommended_size', rec.get('current_size'))}",
                delta_color="off",
            )
            c3.metric(
                "Annual Savings",
                f"${savings_usd:,.0f}",
                delta=f"{savings_credits:,.1f} credits",
                delta_color="normal",
            )

            st.markdown("**Savings Calculation:**")
            st.code(rec.get("savings_calculation_detail", "N/A"), language="text")

            issues = rec.get("issues", [])
            if issues:
                st.markdown("**Issues Detected:**")
                for issue in issues:
                    st.markdown(f"- {issue}")

            sql_cmds = rec.get("alter_sql", [])
            if sql_cmds:
                st.markdown("**Recommended SQL:**")
                for sql in sql_cmds:
                    st.code(sql, language="sql")


def page_anomaly_detection(conn, credit_price: float):
    st.markdown(
        '<div class="section-header">🚨 Anomaly Detection — Z-Score Analysis</div>',
        unsafe_allow_html=True,
    )

    data = load_anomaly_data(conn)
    ts_df   = data["timeseries"]
    spikes  = data["spikes"]
    creep   = data["creep"]

    if ts_df.empty:
        st.warning("No anomaly data. Run: python src/generate_sample_data.py")
        return

    warehouses = sorted(ts_df["warehouse_name"].unique())
    selected_wh = st.selectbox("Select Warehouse", warehouses)

    wh_df = ts_df[ts_df["warehouse_name"] == selected_wh].copy()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=wh_df["usage_date"], y=wh_df["total_credits"],
            name="Credits Used", marker_color="#56ccf2", opacity=0.7,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=wh_df["usage_date"], y=wh_df["z_score"],
            name="Z-Score", line=dict(color="#e74c3c", width=2),
        ),
        secondary_y=True,
    )
    fig.add_hline(y=3.0, line_dash="dash", line_color="#f39c12",
                  annotation_text="Z=3 Threshold", secondary_y=True)
    fig.add_hline(y=-3.0, line_dash="dash", line_color="#f39c12", secondary_y=True)

    fig.update_layout(
        title=f"Z-Score Analysis — {selected_wh}",
        plot_bgcolor="#1a1f2e", paper_bgcolor="#1a1f2e",
        font_color="#c8d6e5", hovermode="x unified",
    )
    fig.update_yaxes(title_text="Credits", gridcolor="#2a3040", secondary_y=False)
    fig.update_yaxes(title_text="Z-Score", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    tab1, tab2 = st.tabs(["🔴 Spikes Detected", "📈 Slow Creep"])

    with tab1:
        if spikes:
            for spike in spikes[:10]:
                wh    = spike.get("warehouse_name", "")
                dt    = spike.get("usage_date", "")
                z     = spike.get("z_score", 0)
                cr    = spike.get("total_credits", 0)
                usd   = cr * credit_price
                st.error(
                    f"🔴 **{wh}** on {dt} — {cr:.1f} credits (${usd:,.0f}) — Z-score: {z:.2f}"
                )
        else:
            st.success("✅ No cost spikes detected in the last 28 days.")

    with tab2:
        if creep:
            for c in creep[:10]:
                wh    = c.get("warehouse_name", "")
                days  = c.get("consecutive_days", 0)
                pct   = c.get("total_increase_pct", 0)
                st.warning(
                    f"📈 **{wh}** — {days} consecutive days of increase — {pct:.1f}% total rise"
                )
        else:
            st.success("✅ No slow creep patterns detected.")


def page_bulk_configurator(conn, credit_price: float):
    st.markdown(
        '<div class="section-header">⚙️ Bulk Configurator — GROUP ALTER + Rollback SQL</div>',
        unsafe_allow_html=True,
    )

    st.info(
        "⚠️ **Production Checklist:** "
        "(1) Download rollback script FIRST. "
        "(2) Apply during low-traffic window. "
        "(3) Monitor for 48 hours after changes."
    )

    # FIX: credit_price pass केला
    grouped_data = load_configurator_data(conn, credit_price)

    if not grouped_data:
        st.warning("No configurator data. Run: python src/generate_sample_data.py")
        return

    all_alter_sql    = []
    all_rollback_sql = []

    for workload_type, group in grouped_data.items():
        warehouses    = group.get("warehouses", [])
        alter_sqls    = group.get("alter_sqls", [])
        rollback_sqls = group.get("rollback_sqls", [])
        total_savings = group.get("total_annual_savings_usd", 0)

        all_alter_sql.extend(alter_sqls)
        all_rollback_sql.extend(rollback_sqls)

        with st.expander(
            f"**{workload_type}** — {len(warehouses)} warehouse(s) — "
            f"💰 ${total_savings:,.0f}/yr potential savings",
            expanded=False,
        ):
            st.markdown(f"**Warehouses:** {', '.join(warehouses)}")

            suspend_map = {
                "BI":     "300s (5 min)",
                "ETL":    "120s (2 min)",
                "AD_HOC": "60s (1 min)",
                "DS":     "600s (10 min)",
            }
            st.info(f"⏱️ Recommended Auto-Suspend: {suspend_map.get(workload_type, '120s (2 min)')}")

            if alter_sqls:
                st.markdown("**ALTER SQL Commands:**")
                st.code("\n\n".join(alter_sqls), language="sql")

            if rollback_sqls:
                st.markdown("**Rollback SQL Commands:**")
                st.code("\n\n".join(rollback_sqls), language="sql")

    st.markdown("---")
    col1, col2 = st.columns(2)

    if all_alter_sql:
        alter_content = (
            "-- Snowflake FinOps Toolkit — Bulk ALTER Script\n"
            "-- Author: Shailesh Chalke\n"
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


def page_whatif_simulator(conn, credit_price: float):
    st.markdown(
        '<div class="section-header">🔮 What-If Simulator — Cost Scenario Modeling</div>',
        unsafe_allow_html=True,
    )

    st.info(
        "💡 **BI Cache Tip:** BI warehouses use result caching. "
        "Reducing auto-suspend below 300s may invalidate the result cache. "
        "Verify cache hit rate before reducing."
    )

    st.markdown("### 🎛️ Scenario Parameters")

    col1, col2, col3 = st.columns(3)

    with col1:
        num_warehouses      = st.slider("Number of Warehouses", 1, 50, 12)
        avg_credits_per_day = st.slider("Avg Credits/Warehouse/Day", 1.0, 200.0, 24.0, step=1.0)

    with col2:
        current_suspend = st.slider("Current Auto-Suspend (seconds)", 60, 3600, 600, step=60)
        new_suspend     = st.slider("New Auto-Suspend (seconds)",     60, 3600, 120, step=60)

    with col3:
        idle_pct       = st.slider("Idle Time % (current)",    0, 100, 35)
        rightsize_pct  = st.slider("Right-Sizing Reduction %", 0, 80,  25)

    bi_pct = st.slider("% Warehouses that are BI (cache-sensitive)", 0, 100, 30)

    # FIX: copy.copy() वापरला — Streamlit mutation bug prevent
    scenario = copy.copy({
        "num_warehouses":      num_warehouses,
        "avg_credits_per_day": avg_credits_per_day,
        "current_suspend":     current_suspend,
        "new_suspend":         new_suspend,
        "idle_pct":            idle_pct,
        "rightsize_pct":       rightsize_pct,
        "credit_price":        credit_price,
        "bi_pct":              bi_pct,
    })

    annual_base_credits = scenario["num_warehouses"] * scenario["avg_credits_per_day"] * 365

    suspend_reduction = max(
        0.0,
        (scenario["current_suspend"] - scenario["new_suspend"])
        / max(scenario["current_suspend"], 1),
    )
    idle_saving_fraction       = (scenario["idle_pct"] / 100) * suspend_reduction
    auto_suspend_savings       = annual_base_credits * idle_saving_fraction
    rightsize_savings          = annual_base_credits * (scenario["rightsize_pct"] / 100)

    bi_warehouses    = round(scenario["num_warehouses"] * scenario["bi_pct"] / 100)
    bi_cache_penalty = 0.0
    if scenario["new_suspend"] < 300 and bi_warehouses > 0:
        bi_cache_penalty = bi_warehouses * scenario["avg_credits_per_day"] * 365 * 0.05

    total_savings_credits = auto_suspend_savings + rightsize_savings - bi_cache_penalty
    total_savings_usd     = total_savings_credits * scenario["credit_price"]
    annual_base_usd       = annual_base_credits   * scenario["credit_price"]
    savings_pct           = (total_savings_usd / max(annual_base_usd, 1)) * 100

    st.markdown("---")
    st.markdown("### 📊 Simulation Results")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Annual Baseline Cost",    f"${annual_base_usd:,.0f}",                                  f"{annual_base_credits:,.0f} credits")
    r2.metric("Auto-Suspend Savings",    f"${auto_suspend_savings * credit_price:,.0f}",               f"{auto_suspend_savings:,.0f} credits")
    r3.metric("Right-Sizing Savings",    f"${rightsize_savings * credit_price:,.0f}",                  f"{rightsize_savings:,.0f} credits")
    r4.metric("Net Annual Savings",      f"${total_savings_usd:,.0f}",                                 f"{savings_pct:.1f}% reduction")

    if bi_cache_penalty > 0:
        st.warning(
            f"⚠️ BI Cache Risk Penalty: ${bi_cache_penalty * credit_price:,.0f}/yr "
            f"({bi_warehouses} BI warehouse(s) with auto-suspend < 300s — ~5% cache-miss overhead)"
        )

    st.markdown("### 📐 Formula Breakdown")
    st.markdown('<div class="sim-box">', unsafe_allow_html=True)
    st.code(
        f"ANNUAL BASELINE\n"
        f"  = {num_warehouses} warehouses x {avg_credits_per_day} cr/day x 365\n"
        f"  = {annual_base_credits:,.0f} credits  (${annual_base_usd:,.0f})\n\n"
        f"AUTO-SUSPEND SAVINGS\n"
        f"  Suspend reduction ratio = ({current_suspend}s - {new_suspend}s) / {current_suspend}s = {suspend_reduction:.2%}\n"
        f"  Idle saving fraction    = {idle_pct}% idle x {suspend_reduction:.2%} = {idle_saving_fraction:.2%}\n"
        f"  Auto-suspend savings    = {annual_base_credits:,.0f} x {idle_saving_fraction:.2%} = {auto_suspend_savings:,.1f} credits\n\n"
        f"RIGHT-SIZING SAVINGS\n"
        f"  = {annual_base_credits:,.0f} x {rightsize_pct}% = {rightsize_savings:,.1f} credits\n\n"
        f"BI CACHE PENALTY\n"
        f"  = {bi_warehouses} BI warehouses x {avg_credits_per_day} x 365 x 5% = {bi_cache_penalty:,.1f} credits\n\n"
        f"NET ANNUAL SAVINGS\n"
        f"  = {auto_suspend_savings:,.1f} + {rightsize_savings:,.1f} - {bi_cache_penalty:,.1f}\n"
        f"  = {total_savings_credits:,.1f} credits  (${total_savings_usd:,.0f})\n"
        f"  = {savings_pct:.1f}% reduction",
        language="text",
    )
    st.markdown("</div>", unsafe_allow_html=True)


def main():
    try:
        conn = get_connector()
    except Exception as e:
        st.error(
            f"❄️ Snowflake Connection Failed: {e}\n\n"
            "Check your .env file and run: python src/generate_sample_data.py"
        )
        st.stop()

    # FIX: sidebar आधी call करा — credit_price आणि page दोन्ही एकत्र मिळतात
    page, credit_price = render_sidebar(conn)

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