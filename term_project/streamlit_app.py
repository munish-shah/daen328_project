import os
from dotenv import load_dotenv

import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# ------------------------------------------------------------------------------
# 1. Load .env and Database Credentials
# ------------------------------------------------------------------------------
load_dotenv()

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

# ------------------------------------------------------------------------------
# 2. Cached Engine & Query Helpers
# ------------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    """Create a SQLAlchemy engine (cached across reruns)."""
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return engine

@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame (cached for 1h)."""
    engine = get_engine()
    return pd.read_sql(sql, engine)

# ------------------------------------------------------------------------------
# 3. Streamlit Page Setup
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Chicago Food Inspections",
    page_icon="🏙️",
    layout="wide"
)

st.title("🏙️ Chicago Food Inspections Dashboard")
st.markdown(
    """
    Explore the City of Chicago's food‐service inspection data.
    Data is pulled directly from your PostgreSQL `inspections` table.
    """
)

# ------------------------------------------------------------------------------
# 4. Load & Prepare Data
# ------------------------------------------------------------------------------
df = run_query("SELECT * FROM inspections;")
df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")

# ------------------------------------------------------------------------------
# 5. Visualization 1: Map of Inspection Locations
# ------------------------------------------------------------------------------

import pgeocode

with st.expander("📍 Inspections Results by Zip Code", expanded=True):
    # 1) Prepare zip & results
    z = df[["zip", "results"]].dropna()
    z["zip"] = z["zip"].astype(int).astype(str).str.zfill(5)

    # 2) Count & pct per result
    ct = z.groupby(["zip", "results"]).size().unstack(fill_value=0)
    tot = ct.sum(axis=1).rename("total_inspections")
    pct = (ct.div(tot, axis=0) * 100).round(1)
    mp = pct.assign(total_inspections=tot).reset_index()

    # 3) Geocode ZIP centroids via pgeocode
    nomi = pgeocode.Nominatim("us")
    geo = (
        nomi.query_postal_code(mp["zip"].tolist())
            .loc[:, ["postal_code", "latitude", "longitude"]]
            .dropna()
            .rename(columns={
                "postal_code": "zip",
                "latitude": "latitude",
                "longitude": "longitude"
            })
    )
    mp = mp.merge(geo, on="zip", how="inner")

    # 4) Plot: size by # inspections, hue by Pass %
    fig = px.scatter_mapbox(
        mp,
        lat="latitude",
        lon="longitude",
        size="total_inspections",
        color_discrete_sequence=["green"],                    # numeric Pass % column
        hover_data={
            "zip" : True,
            "latitude" : False,
            "longitude" : False,
            "total_inspections": True,
            "Pass":               True,
            "Fail":               True,
            "No Entry":           True,
            "Pass w/ Conditions": True,
            "Not Ready":          True
        },
        zoom=10,
        height=600
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        coloraxis_colorbar={"title": "Pass %"},
        margin=dict(r=0, t=0, l=0, b=0)
    )

    st.plotly_chart(fig, use_container_width=True)



with st.expander("📆 Total Inspections by Year", expanded=True):
    # 1) Extract year from the datetime inspection_date
    df["year"] = df["inspection_date"].dt.year

    # 2) Aggregate counts per year
    yearly = (
        df
        .dropna(subset=["year"])
        .groupby("year")
        .size()
        .reset_index(name="inspections")
    )

    # 3) Plot as a bar chart
    fig_year = px.bar(
        yearly,
        x="year",
        y="inspections",
        title="Total Chicago Food Inspections by Year",
        labels={"year": "Year", "inspections": "Number of Inspections"},
        color="inspections",
        color_continuous_scale="Blues"
    )
    fig_year.update_layout(
        xaxis=dict(type="category"),
        margin=dict(r=0, t=40, l=0, b=0),
        coloraxis_showscale=False
    )

    # 4) Render
    st.plotly_chart(fig_year, use_container_width=True)


with st.expander("🏆 Restaurants Ranked by Fail Rate", expanded=True):
    # 1. Clean and normalize result labels
    df["results_clean"] = df["results"].str.strip().str.lower()


    # 2. Group by restaurant name
    result_summary = (
        df.groupby("dba_name")
        .agg(
            total_inspections=("inspection_id", "count"),
            fail_count=("results_clean", lambda x: (x == "fail").sum())
        )
        .reset_index()
    )


    # 3. Compute fail rate
    result_summary["fail_rate"] = (result_summary["fail_count"] / result_summary["total_inspections"]).round(2)


    # 4. Let user choose sort order
    sort_order = st.radio("Sort Order", ["Highest Fail Rate", "Lowest Fail Rate"], horizontal=True)
    ascending = sort_order == "Lowest Fail Rate"
    sorted_results = result_summary.sort_values("fail_rate", ascending=ascending)


    # 5. Optional filter for minimum inspection count
    min_inspections = st.slider("Minimum Number of Inspections", 1, int(result_summary["total_inspections"].max()), 3)
    filtered = sorted_results[sorted_results["total_inspections"] >= min_inspections]


    # 6. Display results
    st.dataframe(
        filtered[["dba_name", "total_inspections", "fail_count", "fail_rate"]]
        .reset_index(drop=True),
        use_container_width=True
    )


# ------------------------------------------------------------------------------
# 10. Visualization: Chain Inspection Count Over Time + Outcome Hover Breakdown
# ------------------------------------------------------------------------------
with st.expander("🍔 Chain Restaurant Monthly Trends (Volume + Outcome %)", expanded=True):
    # Normalize names
    df["dba_name"] = df["dba_name"].str.strip().str.lower()

    # Get valid chains: >5 licenses AND ≥50 inspections
    license_counts = (
        df[["dba_name", "license_"]]
        .dropna()
        .drop_duplicates()
        .groupby("dba_name")["license_"]
        .nunique()
    )
    inspection_counts = df["dba_name"].value_counts()
    valid_chains = license_counts[
        (license_counts > 5) & (inspection_counts >= 50).reindex(license_counts.index, fill_value=0)
    ].index.tolist()

    # User selects a chain
    selected_chain = st.selectbox(
        "Select a Chain (min 5 locations & 50 inspections):",
        sorted(valid_chains)
    )

    # Filter chain data
    chain_df = df[
        (df["dba_name"] == selected_chain) &
        df["results"].notna() &
        df["inspection_date"].notna()
    ].copy()
    chain_df["inspection_month"] = pd.to_datetime(chain_df["inspection_date"]).dt.to_period("M").astype(str)

    # Group: count outcomes per month
    grouped = chain_df.groupby(["inspection_month", "results"]).size().unstack(fill_value=0)

    # Calculate total inspections and percentages
    grouped["total"] = grouped.sum(axis=1)
    for col in ["Pass", "Fail", "Pass w/ Conditions"]:
        if col not in grouped.columns:
            grouped[col] = 0  # ensure column exists if missing

    grouped["hover"] = (
        "Pass: " + (grouped["Pass"] / grouped["total"] * 100).round(1).astype(str) + "%" + "<br>" +
        "Fail: " + (grouped["Fail"] / grouped["total"] * 100).round(1).astype(str) + "%" + "<br>" +
        "Pass w/ Conditions: " + (grouped["Pass w/ Conditions"] / grouped["total"] * 100).round(1).astype(str) + "%"
    )
    grouped["inspection_month"] = grouped.index

    # Plot: y = inspection count, hover = percentages
    fig_chain = px.line(
        grouped,
        x="inspection_month",
        y="total",
        title=f"Monthly Inspection Volume for '{selected_chain.title()}'",
        labels={"inspection_month": "Inspection Month", "total": "Total Inspections"},
        markers=True
    )

    # Update hover data
    fig_chain.update_traces(
        hovertemplate="<b>Month: %{x}</b><br>" +
                     "Total Inspections: %{y}<br>" +
                     "<b>Pass Rate: %{customdata[0]}%</b><br>" +
                     "Fail Rate: %{customdata[1]}%<br>" +
                     "Pass w/ Conditions: %{customdata[2]}%<extra></extra>",
        customdata=grouped[["hover"]]
    )

    fig_chain.update_layout(
        height=600,
        margin={"r": 0, "t": 50, "l": 0, "b": 100},
        showlegend=False,
        xaxis=dict(tickangle=-45)
    )

    st.plotly_chart(fig_chain, use_container_width=True)

# ------------------------------------------------------------------------------
# 8. Visualization: Most Common Violation Types by Zip Code (Violation # on X, Name on Hover)
# ------------------------------------------------------------------------------
with st.expander("📍 Most Common Violations by Zip Code", expanded=True):
    # Get zip options
    zip_options = df["zip"].dropna().unique()
    selected_zip = st.selectbox("Select a Zip Code", sorted(zip_options))

    # Filter rows that match the selected zip
    filtered_df = df[(df["zip"] == selected_zip) & df["violations_list"].notna()]

    # Flatten list of violations for selected zip
    all_violations = pd.Series(
        [v.strip() for sublist in filtered_df["violations_list"] for v in sublist if v]
    )

    # Count and limit to top N
    top_n = st.slider("Top N Violations", min_value=5, max_value=50, value=15)
    violation_counts = all_violations.value_counts().reset_index()
    violation_counts.columns = ["violation_full", "count"]
    top_violation_counts = violation_counts.head(top_n).copy()

    # Parse violation number and name
    def extract_num_and_name(v):
        try:
            # Try to split by dot first
            number = v.split(".", 1)[0].strip()
            name_part = v.split(".", 1)[1].strip()
            # Try to split by dash
            if " - " in name_part:
                name = name_part.split(" - ")[0].strip()
            else:
                name = name_part
            return pd.Series([number, name])
        except:
            # If parsing fails, return empty values
            return pd.Series(["", ""])

    top_violation_counts[["violation_number", "violation_name"]] = top_violation_counts["violation_full"].apply(extract_num_and_name)

    # Plot
    # Sort by count in descending order for better visualization
    top_violation_counts = top_violation_counts.sort_values('count', ascending=False)
    
    fig = px.bar(
        top_violation_counts,
        x="violation_number",
        y="count",
        title=f"Top {top_n} Violations in Zip Code {selected_zip}",
        labels={"violation_number": "Violation Number", "count": "Count"},
        color="count",
        color_continuous_scale="Purples",
        hover_data={"violation_name": True, "violation_number": False, "count": False}
    )

    # Update hover template to show violation name
    fig.update_traces(
        hovertemplate="<b>Violation: %{customdata[0]}</b><br>" +
                     "Count: %{y}<extra></extra>",
        customdata=top_violation_counts[['violation_name']]
    )

    fig.update_layout(
        height=600,
        margin={"r": 0, "t": 50, "l": 0, "b": 100},
        showlegend=False
    )

    st.plotly_chart(fig, use_container_width=True)

