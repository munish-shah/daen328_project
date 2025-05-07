import os
from dotenv import load_dotenv

import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import plotly.express as px
import pgeocode


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
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}" +
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return engine

def run_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(sql, engine)

# ------------------------------------------------------------------------------
# 3. Streamlit Page Setup
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Chicago Food Inspections",
    page_icon="\U0001F3D9\uFE0F",
    layout="wide"
)

st.title("\U0001F3D9\uFE0F Chicago Food Inspections Dashboard")
st.markdown("Explore the City of Chicago's food inspection data from PostgreSQL.")

# ------------------------------------------------------------------------------
# 4. Sidebar Filters
# ------------------------------------------------------------------------------
st.sidebar.header("Filters")
date_filter = st.sidebar.date_input("Inspection After", pd.to_datetime("2012-01-01"))

# Dropdown for ZIP codes
zip_options = run_query("SELECT DISTINCT zip FROM inspections ORDER BY zip")
zip_options = zip_options["zip"].dropna().astype(str).tolist()
zip_filter = st.sidebar.selectbox("ZIP Code", ["All"] + zip_options)


# Construct SQL query
query = f"SELECT * FROM inspections WHERE inspection_date > '{date_filter}'"
if zip_filter != "All":
    query += f" AND zip = '{zip_filter}'"

with st.spinner("Loading data..."):
    df = run_query(query)

df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")

# ------------------------------------------------------------------------------
# 5. Map of Inspection Results by Zip Code
# ------------------------------------------------------------------------------
with st.expander("\U0001F4CD Inspections Results by Zip Code", expanded=True):
    z = df[["zip", "results"]].dropna()
    z["zip"] = z["zip"].astype(int).astype(str).str.zfill(5)

    ct = z.groupby(["zip", "results"]).size().unstack(fill_value=0)
    tot = ct.sum(axis=1).rename("total_inspections")
    pct = (ct.div(tot, axis=0) * 100).round(1)
    mp = pct.assign(total_inspections=tot).reset_index()

    nomi = pgeocode.Nominatim("us")
    geo = (
        nomi.query_postal_code(mp["zip"].tolist())
            .loc[:, ["postal_code", "latitude", "longitude"]]
            .dropna()
            .rename(columns={"postal_code": "zip"})
    )
    mp = mp.merge(geo, on="zip", how="inner")

    fig = px.scatter_mapbox(
        mp,
        lat="latitude",
        lon="longitude",
        size="total_inspections",
        color="Pass" if "Pass" in mp else None,
        hover_name="zip",
        hover_data={"total_inspections": True},
        zoom=10,
        height=600
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        margin=dict(r=0, t=0, l=0, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)

 # ------------------------------------------------------------------------------
# 6. Total Inspections by Year
# ------------------------------------------------------------------------------
outcome_options_year = ["Pass", "Fail", "Pass w/ Conditions", "Not Ready", "No Entry"]
outcome_filter_year = st.multiselect("Filter by Outcome (for this chart only)", outcome_options_year, default=outcome_options_year)

with st.expander("\U0001F4C5 Total Inspections by Year", expanded=True):
    filtered_df_year = df[df["results"].isin(outcome_filter_year)].copy()
    filtered_df_year["year"] = filtered_df_year["inspection_date"].dt.year
    yearly = filtered_df_year.dropna(subset=["year"]).groupby("year").size().reset_index(name="inspections")
    fig_year = px.bar(
        yearly,
        x="year",
        y="inspections",
        title="Total Inspections by Year",
        labels={"year": "Year", "inspections": "Number of Inspections"},
        color="inspections",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_year, use_container_width=True) 

# ------------------------------------------------------------------------------
# 7. Restaurants Ranked by Fail Rate
# ------------------------------------------------------------------------------
with st.expander("\U0001F3C6 Restaurants by Fail Rate", expanded=True):
    df["results_clean"] = df["results"].str.strip().str.lower()
    result_summary = (
        df.groupby("dba_name")
        .agg(total=("inspection_id", "count"), fail=("results_clean", lambda x: (x == "fail").sum()))
        .reset_index()
    )
    result_summary["fail_rate"] = (result_summary["fail"] / result_summary["total"]).round(2)

    sort_order = st.radio("Sort Order", ["Highest", "Lowest"], horizontal=True)
    ascending = sort_order == "Lowest"
    min_inspections = st.slider("Min Inspections", 1, int(result_summary["total"].max()), 5)
    filtered = result_summary[result_summary["total"] >= min_inspections].sort_values("fail_rate", ascending=ascending)

    st.dataframe(filtered, use_container_width=True)

# ------------------------------------------------------------------------------
# 8. Pie Chart
# ------------------------------------------------------------------------------
with st.expander("Distribution of Inspection Results", expanded=True):
    # Count inspection outcomes
    result_counts = df["results"].value_counts().reset_index()
    result_counts.columns = ["Result", "Count"]

    # Pie chart
    fig_pie = px.pie(
        result_counts,
        names="Result",
        values="Count",
        title="Proportion of Inspection Outcomes",
        hole=0.4  
    )

    fig_pie.update_traces(textposition="inside", textinfo="percent+label")

    st.plotly_chart(fig_pie, use_container_width=True)


# ------------------------------------------------------------------------------
# 13. Most Common Inspection Types
# ------------------------------------------------------------------------------

with st.expander("\U0001F50D Most Common Inspection Types", expanded=True):
    if "inspection_type" in df.columns:
        top_n_types = st.slider("Top N Inspection Types", min_value=3, max_value=20, value=10)
        type_counts = (
            df["inspection_type"]
            .dropna()
            .str.strip()
            .value_counts()
            .head(top_n_types)
            .reset_index()
        )
        type_counts.columns = ["Inspection Type", "Count"]

        fig_types = px.bar(
            type_counts,
            x="Inspection Type",
            y="Count",
            title=f"Top {top_n_types} Inspection Types",
            text_auto=True,
            color_discrete_sequence=["#FFA15A"]  # orange
        )
        fig_types.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_types, use_container_width=True)
    else:
        st.warning("Inspection type data not available.")
