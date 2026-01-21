from datetime import datetime
from io import BytesIO
from typing import Any, Optional

import polars as pl
import streamlit as st

def get_grain_overlay() -> str:
    
    return 

def render_kpi_card(
    title: str,
    value: Any,
    delta: Optional[Any] = None,
    delta_color: str = "normal",
    help_text: Optional[str] = None
) -> None:
    
    st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=help_text
    )

def render_kpi_ticker(summary: dict) -> None:
    
    grain = get_grain_overlay()
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        pincodes = summary.get('total_pincodes', 0)
        st.markdown(f, unsafe_allow_html=True)
    
    with col2:
        saturation = summary.get('avg_saturation_rate', 0) * 100
        st.markdown(f, unsafe_allow_html=True)
    
    with col3:
        volatile_count = summary.get('volatile_camp_count', 0)
        st.markdown(f, unsafe_allow_html=True)
    
    with col4:
        migration = summary.get('migration_hotspot_count', 0)
        st.markdown(f, unsafe_allow_html=True)
    
    with col5:
        fraud_count = summary.get('high_fraud_risk_count', 0)
        st.markdown(f, unsafe_allow_html=True)

def render_cluster_summary(
    emerging: int,
    saturated: int,
    high_churn: int
) -> None:
    
    grain = get_grain_overlay()
    total = emerging + saturated + high_churn
    if total == 0:
        st.info("No cluster data available")
        return
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        pct = (emerging / total * 100) if total > 0 else 0
        st.markdown(f, unsafe_allow_html=True)
    
    with col2:
        pct = (saturated / total * 100) if total > 0 else 0
        st.markdown(f, unsafe_allow_html=True)
    
    with col3:
        pct = (high_churn / total * 100) if total > 0 else 0
        st.markdown(f, unsafe_allow_html=True)

def render_watchlist_table(
    df: pl.DataFrame,
    title: str = "Risk Watchlist"
) -> Optional[str]:
    
    st.subheader(title)
    
    if df.is_empty():
        st.info("No high-risk districts detected")
        return None
    
    risk_df = df.filter(
        (pl.col("avg_dhr") > 1.5) | (pl.col("avg_mii") > 0.4)
    )
    
    if risk_df.is_empty():
        st.success("All districts within normal parameters")
        return None
    
    display_df = risk_df.select([
        pl.col("district").alias("District"),
        pl.col("state").alias("State"),
        pl.col("avg_mii").round(3).alias("MII"),
        pl.col("avg_dhr").round(3).alias("DHR"),
        pl.col("volatile_camp_count").alias("Volatile"),
        pl.col("fraud_risk_count").alias("Fraud Risk")
    ])
    
    pandas_df = display_df.to_pandas()
    
    def get_risk_level(row):
        if row["MII"] > 0.4 and row["DHR"] > 1.5:
            return "CRITICAL"
        elif row["MII"] > 0.4 or row["DHR"] > 1.5:
            return "HIGH"
        else:
            return "MODERATE"
    
    pandas_df["Risk Level"] = pandas_df.apply(get_risk_level, axis=1)
    pandas_df = pandas_df[["Risk Level", "District", "State", "MII", "DHR", "Volatile", "Fraud Risk"]]
    
    st.dataframe(
        pandas_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Risk Level": st.column_config.TextColumn("Risk Level"),
            "MII": st.column_config.NumberColumn("MII", format="%.3f"),
            "DHR": st.column_config.NumberColumn("DHR", format="%.3f"),
        }
    )
    
    return None

def render_pincode_table(
    df: pl.DataFrame,
    sort_col: str = "ovs",
    ascending: bool = False
) -> Optional[str]:
    
    if df.is_empty():
        st.info("No pincode data available")
        return None
    
    search = st.text_input("Search Pincode", placeholder="Enter 6-digit pincode...")
    
    filtered_df = df
    if search:
        filtered_df = df.filter(pl.col("pincode").str.contains(search))
    
    sorted_df = filtered_df.sort(sort_col, descending=not ascending)
    
    display_df = sorted_df.head(100).select([
        pl.col("pincode").alias("Pincode"),
        pl.col("district").alias("District"),
        pl.col("state").alias("State"),
        pl.col("ovs").round(3).alias("OVS"),
        pl.col("ovs_classification").alias("Classification"),
        pl.col("mii").round(3).alias("MII"),
        pl.col("is_volatile_camp").alias("Volatile")
    ])
    
    pandas_df = display_df.to_pandas()
    
    st.dataframe(
        pandas_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "OVS": st.column_config.NumberColumn("OVS", format="%.3f"),
            "MII": st.column_config.NumberColumn("MII", format="%.3f"),
            "Volatile": st.column_config.CheckboxColumn("Camp"),
        }
    )
    
    selected = st.selectbox(
        "Select pincode for details:",
        options=[""] + sorted_df["pincode"].to_list()[:50],
        index=0
    )
    
    return selected if selected else None

def generate_vigilance_report(
    risk_districts: pl.DataFrame,
    summary: dict
) -> BytesIO:
    
    buffer = BytesIO()
    
    report = []
    report.append("=" * 60)
    report.append("AADHAR PULSE - VIGILANCE REPORT")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 60)
    report.append("")
    
    report.append("NATIONAL SUMMARY")
    report.append("-" * 40)
    report.append(f"Total Districts: {summary.get('total_districts', 0)}")
    report.append(f"Total Pincodes: {summary.get('total_pincodes', 0)}")
    report.append(f"Volatile Camps Detected: {summary.get('volatile_camp_count', 0)}")
    report.append(f"Migration Hotspots: {summary.get('migration_hotspot_count', 0)}")
    report.append(f"High Fraud Risk Areas: {summary.get('high_fraud_risk_count', 0)}")
    report.append("")
    
    report.append("HIGH-RISK DISTRICTS")
    report.append("-" * 40)
    
    if risk_districts.is_empty():
        report.append("No high-risk districts identified.")
    else:
        for i, row in enumerate(risk_districts.iter_rows(named=True), 1):
            report.append(f"\n{i}. {row.get('district', 'Unknown')} ({row.get('state', 'Unknown')})")
            report.append(f"   MII: {row.get('avg_mii', 0):.3f} | DHR: {row.get('avg_dhr', 0):.3f}")
            report.append(f"   Volatile Camps: {row.get('volatile_camp_count', 0)}")
            report.append(f"   Fraud Risk Count: {row.get('fraud_risk_count', 0)}")
    
    report.append("")
    report.append("=" * 60)
    report.append("RECOMMENDATIONS")
    report.append("-" * 40)
    
    if summary.get('volatile_camp_count', 0) > 0:
        report.append("* Deploy mobile verification units to identified camps")
    
    if summary.get('migration_hotspot_count', 0) > 0:
        report.append("* Coordinate with labor department for migration tracking")
    
    if summary.get('high_fraud_risk_count', 0) > 0:
        report.append("* Initiate vigilance audit in high DHR districts")
        report.append("* Cross-verify demographic updates with biometric records")
    
    report.append("")
    report.append("=" * 60)
    report.append("END OF REPORT")
    
    content = "\n".join(report)
    buffer.write(content.encode("utf-8"))
    buffer.seek(0)
    
    return buffer

def render_recommendation_card(
    pincode: str,
    ovs: float,
    tlp_classification: str,
    recommendation: str
) -> None:
    
    grain = get_grain_overlay()
    
    if ovs > 4.0:
        gradient = "red"
        status = "TEMPORARY CAMP DETECTED"
    elif ovs < 0.5:
        gradient = "green"
        status = "PERMANENT CENTER"
    else:
        gradient = "blue"
        status = "NORMAL OPERATIONS"
    
    st.markdown(f, unsafe_allow_html=True)
