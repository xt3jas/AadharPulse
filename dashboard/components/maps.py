from typing import Optional

import altair as alt
import polars as pl

def create_maturity_scatter(
    df: pl.DataFrame,
    x_col: str = "normalized_enrolment_rate",
    y_col: str = "normalized_update_rate",
    color_col: str = "sml_label",
    tooltip_cols: Optional[list[str]] = None
) -> alt.Chart:
    
    if df.is_empty():
        return alt.Chart().mark_text().encode(
            text=alt.value("No data available")
        )
    
    pandas_df = df.to_pandas()
    
    if tooltip_cols is None:
        tooltip_cols = ["district", "state", x_col, y_col, color_col]
    
    available_tooltips = [c for c in tooltip_cols if c in pandas_df.columns]
    
    cluster_colors = {
        "Emerging": "#10B981",      # Green - growth
        "Saturated": "#3B82F6",     # Blue - stable
        "High Churn": "#EF4444",    # Red - volatile
    }
    
    domain = list(cluster_colors.keys())
    range_colors = list(cluster_colors.values())
    
    chart = (
        alt.Chart(pandas_df)
        .mark_circle(size=150, opacity=0.7, stroke="#374151", strokeWidth=1)
        .encode(
            x=alt.X(
                f"{x_col}:Q",
                title="Enrolment Rate (Normalized)",
                scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(format=".0%", grid=True, gridOpacity=0.3)
            ),
            y=alt.Y(
                f"{y_col}:Q",
                title="Update Rate (Normalized)",
                scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(format=".0%", grid=True, gridOpacity=0.3)
            ),
            color=alt.Color(
                f"{color_col}:N",
                title="Maturity Level",
                scale=alt.Scale(domain=domain, range=range_colors),
                legend=alt.Legend(orient="bottom", columns=3)
            ),
            tooltip=[alt.Tooltip(c, title=c.replace("_", " ").title()) for c in available_tooltips],
            size=alt.Size(
                "total_enrolment:Q" if "total_enrolment" in pandas_df.columns else alt.value(150),
                title="Total Enrolment",
                scale=alt.Scale(range=[50, 500]),
                legend=None
            )
        )
        .properties(
            width=600,
            height=400,
            title=alt.TitleParams(
                text="District Maturity Map",
                subtitle="Click to filter dashboard",
                fontSize=18,
                anchor="start"
            )
        )
        .interactive()
    )
    
    return chart

def create_tlp_chart(
    tlp_data: dict,
    title: str = "Weekly Load Profile"
) -> alt.Chart:
    
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    short_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    
    data = []
    for i, (day, short) in enumerate(zip(days, short_days)):
        key = day.lower()
        value = tlp_data.get(key, 0)
        data.append({
            "day": short,
            "day_full": day,
            "order": i,
            "percentage": value,
            "is_weekend": i >= 5
        })
    
    df = pl.DataFrame(data).to_pandas()
    
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X(
                "day:N",
                title="Day of Week",
                sort=short_days,
                axis=alt.Axis(labelAngle=0)
            ),
            y=alt.Y(
                "percentage:Q",
                title="Volume Percentage",
                axis=alt.Axis(format=".0%"),
                scale=alt.Scale(domain=[0, max(0.5, max(v["percentage"] for v in data) * 1.2)])
            ),
            color=alt.condition(
                alt.datum.is_weekend,
                alt.value("#8B5CF6"),  # Purple for weekends
                alt.value("#3B82F6")   # Blue for weekdays
            ),
            tooltip=[
                alt.Tooltip("day_full:N", title="Day"),
                alt.Tooltip("percentage:Q", title="Volume", format=".1%")
            ]
        )
        .properties(
            width=400,
            height=250,
            title=alt.TitleParams(
                text=title,
                fontSize=14,
                anchor="start"
            )
        )
    )
    
    return chart

def create_ovs_bar_chart(
    df: pl.DataFrame,
    pincode_col: str = "pincode",
    ovs_col: str = "ovs",
    limit: int = 20
) -> alt.Chart:
    
    if df.is_empty():
        return alt.Chart().mark_text().encode(text=alt.value("No data"))
    
    top_df = df.sort(ovs_col, descending=True).head(limit)
    
    top_df = top_df.with_columns([
        pl.when(pl.col(ovs_col) > 4.0)
        .then(pl.lit("High"))
        .when(pl.col(ovs_col) < 0.5)
        .then(pl.lit("Low"))
        .otherwise(pl.lit("Medium"))
        .alias("volatility_category")
    ])
    
    pandas_df = top_df.to_pandas()
    
    chart = (
        alt.Chart(pandas_df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X(
                f"{ovs_col}:Q",
                title="Operational Volatility Score",
                axis=alt.Axis(grid=True)
            ),
            y=alt.Y(
                f"{pincode_col}:N",
                title="Pincode",
                sort="-x"
            ),
            color=alt.Color(
                "volatility_category:N",
                scale=alt.Scale(
                    domain=["High", "Medium", "Low"],
                    range=["#EF4444", "#F59E0B", "#10B981"]
                ),
                legend=alt.Legend(title="Volatility")
            ),
            tooltip=[
                alt.Tooltip(f"{pincode_col}:N", title="Pincode"),
                alt.Tooltip(f"{ovs_col}:Q", title="OVS", format=".2f")
            ]
        )
        .properties(
            width=500,
            height=min(400, limit * 25),
            title=alt.TitleParams(
                text="Top Volatile Pincodes",
                fontSize=14,
                anchor="start"
            )
        )
    )
    
    return chart

def create_risk_heatmap(
    df: pl.DataFrame,
    x_col: str = "mii",
    y_col: str = "dhr",
    label_col: str = "district"
) -> alt.Chart:
    
    if df.is_empty():
        return alt.Chart().mark_text().encode(text=alt.value("No data"))
    
    pandas_df = df.to_pandas()
    
    points = (
        alt.Chart(pandas_df)
        .mark_circle(size=100, opacity=0.7)
        .encode(
            x=alt.X(
                f"{x_col}:Q",
                title="Migration Impact Index",
                scale=alt.Scale(domain=[0, 1])
            ),
            y=alt.Y(
                f"{y_col}:Q",
                title="Data Hygiene Ratio",
                scale=alt.Scale(domain=[0, 3])
            ),
            color=alt.condition(
                (alt.datum[x_col] > 0.4) | (alt.datum[y_col] > 1.5),
                alt.value("#EF4444"),
                alt.value("#10B981")
            ),
            tooltip=[
                alt.Tooltip(f"{label_col}:N", title="District"),
                alt.Tooltip(f"{x_col}:Q", title="MII", format=".2f"),
                alt.Tooltip(f"{y_col}:Q", title="DHR", format=".2f")
            ]
        )
    )
    
    mii_rule = (
        alt.Chart(pl.DataFrame({"x": [0.4]}).to_pandas())
        .mark_rule(color="#EF4444", strokeDash=[5, 5], strokeWidth=2)
        .encode(x=alt.X("x:Q"))
    )
    
    dhr_rule = (
        alt.Chart(pl.DataFrame({"y": [1.5]}).to_pandas())
        .mark_rule(color="#EF4444", strokeDash=[5, 5], strokeWidth=2)
        .encode(y=alt.Y("y:Q"))
    )
    
    chart = (points + mii_rule + dhr_rule).properties(
        width=500,
        height=400,
        title="Risk Assessment Matrix"
    )
    
    return chart

def create_cluster_pie_chart(
    emerging: int,
    saturated: int,
    high_churn: int
) -> alt.Chart:
    
    data = pl.DataFrame({
        "category": ["Emerging", "Saturated", "High Churn"],
        "count": [emerging, saturated, high_churn],
        "color": ["#10B981", "#3B82F6", "#EF4444"]
    }).to_pandas()
    
    chart = (
        alt.Chart(data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("count:Q"),
            color=alt.Color(
                "category:N",
                scale=alt.Scale(
                    domain=["Emerging", "Saturated", "High Churn"],
                    range=["#10B981", "#3B82F6", "#EF4444"]
                ),
                legend=alt.Legend(orient="bottom")
            ),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("count:Q", title="Districts")
            ]
        )
        .properties(
            width=200,
            height=200,
            title="District Maturity Distribution"
        )
    )
    
    return chart
