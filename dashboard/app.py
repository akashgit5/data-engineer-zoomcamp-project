"""
dashboard/app.py
─────────────────
Streamlit dashboard for US Tariff Analytics.

Two main tiles:
  Tile 1 — Top Countries by Average Tariff Rate (categorical bar chart)
  Tile 2 — Tariff Rate Distribution by Product Sector (horizontal bar chart)

Plus: KPI header, rate bucket histogram, and country×sector explorer.

Run: streamlit run dashboard/app.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH      = PROJECT_ROOT / "warehouse" / "tariffs.db"

st.set_page_config(
    page_title="US Tariff Analytics 2025",
    page_icon="🇺🇸",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2.4rem;
        font-weight: 800;
        color: #B22234;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #3C3B6E;
        margin-top: 0;
        margin-bottom: 1.5rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 12px;
        padding: 18px 22px;
        text-align: center;
        border-left: 5px solid #B22234;
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #B22234; }
    .kpi-label { font-size: 0.85rem; color: #555; text-transform: uppercase; letter-spacing: 1px; }
    .tile-title {
        font-size: 1.25rem;
        font-weight: 700;
        color: #3C3B6E;
        border-bottom: 3px solid #B22234;
        padding-bottom: 6px;
        margin-bottom: 10px;
    }
    .tile-subtitle { font-size: 0.88rem; color: #777; margin-bottom: 12px; }
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ── Data loading ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data():
    """Load all tables from DuckDB into Pandas DataFrames."""
    if not DB_PATH.exists():
        return None

    con = duckdb.connect(str(DB_PATH), read_only=True)

    data = {
        "country":  con.execute("SELECT * FROM tariffs_by_country  ORDER BY avg_tariff_rate DESC").fetchdf(),
        "sector":   con.execute("SELECT * FROM tariffs_by_sector   ORDER BY avg_tariff_rate DESC").fetchdf(),
        "matrix":   con.execute("SELECT * FROM country_sector_matrix").fetchdf(),
        "buckets":  con.execute("SELECT * FROM rate_buckets        ORDER BY rate_bucket").fetchdf(),
        "summary":  con.execute("SELECT * FROM summary_kpis").fetchdf(),
        "raw":      con.execute("SELECT * FROM raw_tariffs         LIMIT 5000").fetchdf(),
    }
    con.close()
    return data


# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/en/thumb/a/a4/Flag_of_the_United_States.svg/320px-Flag_of_the_United_States.svg.png",
             width=80)
    st.markdown("## 🇺🇸 US Tariff Analytics")
    st.markdown("**Data source:** Kaggle — *US Tariffs 2025*  \n**Author:** Daniel Calvo Glez")
    st.markdown("---")
    st.markdown("### Filters")

    data = load_data()

    if data:
        top_n_countries = st.slider("Top N countries (Tile 1)", 5, 30, 15)
        top_n_sectors   = st.slider("Top N sectors (Tile 2)",   5, 30, 15)
        min_products    = st.number_input("Min product lines to include", 1, 500, 1)
    else:
        st.warning("⚠️ No data found. Run the pipeline first:\n```\nmake pipeline\n```")
        st.stop()

    st.markdown("---")
    st.markdown("### Pipeline")
    st.code("make pipeline\nstreamlit run dashboard/app.py", language="bash")
    st.markdown("### SQL Workbench")
    st.code(f"jdbc:duckdb:{DB_PATH}", language="text")


# ── Header ─────────────────────────────────────────────────────────────────

st.markdown('<p class="main-header">🇺🇸 US Tariff Analytics Dashboard 2025</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Exploring the impact of US tariffs on global trade — country, sector, and product-level insights</p>', unsafe_allow_html=True)

# ── KPI Row ────────────────────────────────────────────────────────────────

kpi = data["summary"].iloc[0]

k1, k2, k3, k4, k5 = st.columns(5)

def kpi_card(col, value, label):
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

kpi_card(k1, f"{int(kpi.get('total_product_lines', 0)):,}", "Product Lines")
kpi_card(k2, f"{int(kpi.get('total_countries', 0)):,}", "Trading Partners")
kpi_card(k3, f"{int(kpi.get('total_sectors', 0)):,}", "Product Sectors")
kpi_card(k4, f"{kpi.get('global_avg_tariff', 0):.1f}%", "Avg Tariff Rate")
kpi_card(k5, f"{kpi.get('max_tariff_rate', 0):.0f}%", "Max Tariff Rate")

st.markdown("<br>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TILE 1 — Top Countries by Average Tariff Rate (Categorical Distribution)
# ══════════════════════════════════════════════════════════════════════════════

col1, col2 = st.columns([6, 4], gap="large")

with col1:
    st.markdown('<div class="tile-title">📊 Tile 1 — Average Tariff Rate by Trading Partner</div>', unsafe_allow_html=True)
    st.markdown('<div class="tile-subtitle">Which countries face the highest US tariff burden? Ranked by average tariff rate across all product lines.</div>', unsafe_allow_html=True)

    df_country = (
        data["country"]
        .query(f"product_line_count >= {min_products}")
        .head(top_n_countries)
        .sort_values("avg_tariff_rate", ascending=True)
    )

    fig1 = go.Figure()

    # Error bar = ±1 stddev
    fig1.add_trace(go.Bar(
        x=df_country["avg_tariff_rate"],
        y=df_country["country"],
        orientation="h",
        marker=dict(
            color=df_country["avg_tariff_rate"],
            colorscale="RdYlGn_r",
            colorbar=dict(title="Avg Rate %", thickness=12),
            line=dict(width=0.5, color="white"),
        ),
        error_x=dict(
            type="data",
            array=df_country["stddev_tariff_rate"].fillna(0),
            color="rgba(0,0,0,0.3)",
            thickness=1.5,
        ),
        text=[f"{v:.1f}%" for v in df_country["avg_tariff_rate"]],
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Avg Tariff Rate: %{x:.2f}%<br>"
            "Product Lines: %{customdata[0]:,}<br>"
            "Max Rate: %{customdata[1]:.1f}%<extra></extra>"
        ),
        customdata=df_country[["product_line_count", "max_tariff_rate"]].values,
    ))

    fig1.update_layout(
        height=500,
        margin=dict(l=10, r=60, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            title="Average Tariff Rate (%)",
            gridcolor="#eee",
            ticksuffix="%",
        ),
        yaxis=dict(title="", tickfont=dict(size=12)),
        font=dict(family="Inter, sans-serif"),
    )

    st.plotly_chart(fig1, use_container_width=True)
    st.caption("Error bars = ±1 standard deviation across product lines. Color = tariff severity.")

# ── Tile 1 companion: Rate bucket histogram ─────────────────────────────────
with col2:
    st.markdown('<div class="tile-title">📈 Tariff Rate Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="tile-subtitle">How are tariff rates spread across all product lines globally?</div>', unsafe_allow_html=True)

    df_buckets = data["buckets"].copy()
    bucket_order = ["0–5%", "5–10%", "10–25%", "25–50%", "50–100%", "100%+"]
    df_buckets["rate_bucket"] = pd.Categorical(df_buckets["rate_bucket"], categories=bucket_order, ordered=True)
    df_buckets = df_buckets.sort_values("rate_bucket")

    colors = ["#2ecc71", "#a8e063", "#f39c12", "#e67e22", "#c0392b", "#7b241c"]

    fig_dist = go.Figure(go.Bar(
        x=df_buckets["rate_bucket"],
        y=df_buckets["product_line_count"],
        marker_color=colors[:len(df_buckets)],
        text=df_buckets["product_line_count"].apply(lambda x: f"{x:,}"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Product lines: %{y:,}<br>Countries: %{customdata}<extra></extra>",
        customdata=df_buckets["country_count"],
    ))

    fig_dist.update_layout(
        height=250,
        margin=dict(l=10, r=10, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="Tariff Rate Bracket", gridcolor="#eee"),
        yaxis=dict(title="Product Lines", gridcolor="#eee"),
        font=dict(family="Inter, sans-serif"),
        showlegend=False,
    )
    st.plotly_chart(fig_dist, use_container_width=True)

    # ── Small data table ───────────────────────────────────────────────────
    st.markdown("**Top 5 Countries by Product Line Count**")
    top5 = (
        data["country"]
        .nlargest(5, "product_line_count")[["country", "product_line_count", "avg_tariff_rate"]]
        .rename(columns={"country": "Country", "product_line_count": "Lines", "avg_tariff_rate": "Avg Rate %"})
        .reset_index(drop=True)
    )
    top5.index += 1
    st.dataframe(top5, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# TILE 2 — Tariff Rate by Product Sector (Temporal / Categorical Distribution)
# ══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="tile-title">🏭 Tile 2 — Tariff Impact by Product Sector</div>', unsafe_allow_html=True)
st.markdown('<div class="tile-subtitle">Which product categories are most affected by US tariffs? Size of bubble = number of product lines impacted; color = average tariff rate.</div>', unsafe_allow_html=True)

tab_bar, tab_bubble, tab_heatmap, tab_raw = st.tabs(["Bar Chart", "Bubble Chart", "Country×Sector Heatmap", "Raw Data Explorer"])

# ── Tab 1: Horizontal bar ──────────────────────────────────────────────────
with tab_bar:
    df_sector = (
        data["sector"]
        .query(f"product_line_count >= {min_products}")
        .head(top_n_sectors)
        .sort_values("avg_tariff_rate", ascending=True)
    )

    fig2 = px.bar(
        df_sector,
        x="avg_tariff_rate",
        y="sector",
        orientation="h",
        color="avg_tariff_rate",
        color_continuous_scale="RdYlGn_r",
        text=df_sector["avg_tariff_rate"].apply(lambda x: f"{x:.1f}%"),
        hover_data={"product_line_count": True, "country_count": True, "max_tariff_rate": True},
        labels={
            "avg_tariff_rate":    "Avg Tariff Rate (%)",
            "sector":             "Product Sector",
            "product_line_count": "Product Lines",
            "country_count":      "Countries Affected",
            "max_tariff_rate":    "Max Rate (%)",
        },
    )
    fig2.update_traces(textposition="outside")
    fig2.update_layout(
        height=550,
        margin=dict(l=10, r=50, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="Average Tariff Rate (%)", ticksuffix="%", gridcolor="#eee"),
        yaxis=dict(title=""),
        coloraxis_showscale=False,
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("Sectors with fewer than the minimum product lines are excluded.")

# ── Tab 2: Bubble chart ────────────────────────────────────────────────────
with tab_bubble:
    df_bubble = data["sector"].query(f"product_line_count >= {min_products}").head(top_n_sectors)

    fig_bubble = px.scatter(
        df_bubble,
        x="country_count",
        y="avg_tariff_rate",
        size="product_line_count",
        color="avg_tariff_rate",
        color_continuous_scale="RdYlGn_r",
        hover_name="sector",
        text="sector",
        size_max=60,
        labels={
            "country_count":      "Number of Countries Affected",
            "avg_tariff_rate":    "Average Tariff Rate (%)",
            "product_line_count": "Product Line Count",
        },
    )
    fig_bubble.update_traces(textposition="top center", marker=dict(opacity=0.8))
    fig_bubble.update_layout(
        height=550,
        margin=dict(l=10, r=10, t=30, b=40),
        plot_bgcolor="rgba(245,247,250,1)",
        paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(title="Average Tariff Rate (%)", ticksuffix="%", gridcolor="#ddd"),
        xaxis=dict(title="Number of Countries Affected", gridcolor="#ddd"),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_bubble, use_container_width=True)
    st.caption("Bubble size = number of product lines. Hover for sector details.")

# ── Tab 3: Country × Sector heatmap ───────────────────────────────────────
with tab_heatmap:
    df_matrix = data["matrix"].copy()

    # Filter to top countries and sectors by volume
    top_countries = (
        data["country"]
        .nlargest(min(15, top_n_countries), "product_line_count")["country"].tolist()
    )
    top_sectors = (
        data["sector"]
        .nlargest(min(12, top_n_sectors), "product_line_count")["sector"].tolist()
    )

    df_heat = (
        df_matrix[
            df_matrix["country"].isin(top_countries) &
            df_matrix["sector"].isin(top_sectors)
        ]
        .pivot_table(index="country", columns="sector", values="avg_tariff_rate", aggfunc="mean")
        .fillna(0)
    )

    fig_heat = px.imshow(
        df_heat,
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
        labels=dict(color="Avg Tariff Rate (%)"),
        title="Average Tariff Rate — Country × Product Sector",
    )
    fig_heat.update_layout(
        height=500,
        margin=dict(l=10, r=10, t=50, b=10),
        font=dict(family="Inter, sans-serif"),
        coloraxis_colorbar=dict(ticksuffix="%"),
    )
    fig_heat.update_xaxes(side="bottom", tickangle=-35)
    st.plotly_chart(fig_heat, use_container_width=True)
    st.caption("Cells show average tariff rate (%). Darker red = higher tariff.")

# ── Tab 4: Raw data explorer ───────────────────────────────────────────────
with tab_raw:
    st.markdown("**Browse individual tariff entries**")

    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        countries_list = ["All"] + sorted(data["raw"]["country"].dropna().unique().tolist())
        selected_country = st.selectbox("Filter by Country", countries_list)
    with col_filter2:
        sectors_list = ["All"] + sorted(data["raw"]["sector"].dropna().unique().tolist())
        selected_sector = st.selectbox("Filter by Sector", sectors_list)

    df_raw = data["raw"].copy()
    if selected_country != "All":
        df_raw = df_raw[df_raw["country"] == selected_country]
    if selected_sector != "All":
        df_raw = df_raw[df_raw["sector"] == selected_sector]

    st.dataframe(
        df_raw[["country", "sector", "hs_code", "description", "tariff_rate"]]
        .sort_values("tariff_rate", ascending=False)
        .head(500)
        .reset_index(drop=True),
        use_container_width=True,
        height=400,
    )
    st.caption(f"Showing up to 500 rows (from first 5,000 loaded). Total filtered: {len(df_raw):,}")

# ── Footer ─────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "**US Tariff Analytics 2025** | "
    "Pipeline: Prefect → PySpark → DuckDB | "
    "Dashboard: Streamlit + Plotly | "
    f"Data: [Kaggle — US Tariffs 2025](https://www.kaggle.com/datasets/danielcalvoglez/us-tariffs-2025)"
)
