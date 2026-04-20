import streamlit as st
import pandas as pd
import altair as alt
from datetime import date
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Majors Acq - Revenue Projections",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

session = get_active_session()

# Close rates date range (2-year rolling window)
_today = date.today()
_cr_start = date(_today.year - 2, _today.month, _today.day)
CR_DATE_RANGE = f"{_cr_start.strftime('%b %Y')} - {_today.strftime('%b %Y')}"

# Opportunity stages (matching Salesforce)
STAGES = [
    "SDR Qualified: SQL",
    "Sales Qualified Opportunity",
    "Discovery",
    "Scope / Use Case",
    "Technical / Business Impact Validation",
    "Negotiation",
    "Deal Imminent",
]


@st.cache_data(ttl=300)
def load_close_rates():
    return session.sql(
        "SELECT STAGE, STAGE_ORDER, CLOSE_RATE, DESCRIPTION "
        "FROM SNOWPUBLIC.STREAMLIT.CLOSE_RATES ORDER BY STAGE_ORDER"
    ).to_pandas()


@st.cache_data(ttl=300)
def load_team():
    return session.sql(
        "SELECT AE_NAME, SE_NAME, ROLE, DM_NAME, SEM_NAME, TERRITORY, DISTRICT "
        "FROM SNOWPUBLIC.STREAMLIT.TEAM_ROSTER ORDER BY AE_NAME"
    ).to_pandas()


@st.cache_data(ttl=300)
def load_leadership():
    return session.sql(
        "SELECT ROLE_TYPE, NAME, TITLE "
        "FROM SNOWPUBLIC.STREAMLIT.LEADERSHIP"
    ).to_pandas()


@st.cache_data(ttl=120)
def load_pipeline():
    return session.sql(
        "SELECT OPP_ID, OPPORTUNITY_NAME, ACCOUNT_NAME, AE_NAME, SE_NAME, "
        "       STAGE, STAGE_NUMBER, FISCAL_YEAR, ACV_K, CLOSE_DATE, "
        "       COMPETITORS, NEXT_STEPS, DISTRICT "
        "FROM SNOWPUBLIC.STREAMLIT.PIPELINE "
        "ORDER BY AE_NAME, STAGE_NUMBER"
    ).to_pandas()


# District display names
DISTRICT_LABELS = {
    "ALL": "All Districts",
    "MajorsAcqSouthEast": "SouthEast",
    "MajorsAcqNorthCentral": "NorthCentral",
    "MajorsAcqNortheast": "Northeast",
    "MajorsAcqWest": "West",
}
DISTRICT_OPTIONS = list(DISTRICT_LABELS.keys())


# ============================================================================
# LOAD DATA
# ============================================================================
cr_df = load_close_rates()
team_df = load_team()
leadership_df = load_leadership()
pipeline_df = load_pipeline()

# Build close-rate lookup
cr_defaults = dict(zip(cr_df["STAGE"], cr_df["CLOSE_RATE"]))


# ============================================================================
# DISTRICT SELECTOR
# ============================================================================
selected_district = st.selectbox(
    "Select District",
    options=DISTRICT_OPTIONS,
    format_func=lambda x: f"Majors Acquisition — {DISTRICT_LABELS[x]}",
    index=DISTRICT_OPTIONS.index("MajorsAcqSouthEast"),
)

# Filter pipeline to selected district
if selected_district != "ALL":
    pipeline_df = pipeline_df[pipeline_df["DISTRICT"] == selected_district].copy()

district_display = "All Districts" if selected_district == "ALL" else DISTRICT_LABELS[selected_district]


# ============================================================================
# SIDEBAR: Close-rate sliders + Team roster
# ============================================================================
with st.sidebar:
    st.header("Close-Rate Assumptions")
    st.caption(
        "Adjust stage probabilities below. Defaults based on "
        f"historical Cap 1 win rates from Enterprise & Majors Acquisition opportunities ({CR_DATE_RANGE})."
    )
    close_rates = {}
    for stage in STAGES:
        default_pct = int(round(float(cr_defaults.get(stage, 50.0)) * 100))
        pct = st.slider(
            stage,
            min_value=0,
            max_value=100,
            value=default_pct,
            step=1,
            format="%d%%",
            key=f"cr_{stage}",
        )
        close_rates[stage] = pct / 100.0

    st.markdown("---")
    st.header("Team Roster")
    sales_rvp = leadership_df[leadership_df["ROLE_TYPE"] == "Sales RVP"]
    se_rvp = leadership_df[leadership_df["ROLE_TYPE"] == "SE RVP"]
    rvp_sales_name = sales_rvp.iloc[0]["NAME"] if len(sales_rvp) > 0 else "TBD"
    rvp_se_name = se_rvp.iloc[0]["NAME"] if len(se_rvp) > 0 else "TBD"
    st.markdown(
        f"**RVP Sales:** {rvp_sales_name}  \n"
        f"**RVP SE:** {rvp_se_name}"
    )
    st.markdown("---")
    if selected_district == "ALL":
        roster = team_df
    else:
        roster = team_df[team_df["DISTRICT"] == selected_district]
    if len(roster) > 0:
        if selected_district == "ALL":
            st.caption("All Majors Acquisition Districts")
        else:
            dm_name = roster.iloc[0]["DM_NAME"]
            st.caption(f"{district_display} Majors Acquisition (DM: {dm_name})")
        for _, row in roster.iterrows():
            st.markdown(
                f"**{row['AE_NAME']}** / {row['SE_NAME']}  \n"
                f"DM: {row['DM_NAME']} | SEM: {row['SEM_NAME']}"
            )


# ============================================================================
# MAIN: Title
# ============================================================================
st.title(f"Majors Acquisition — {district_display}")
st.subheader("Revenue Projections — Open Opportunities (FY26/FY27)")
st.caption(
    f"Pipeline weighted by Enterprise & Majors Acquisition Cap 1 historical win rates ({CR_DATE_RANGE}). "
    "Opportunity-level data. Adjust close rates in the sidebar."
)


# ============================================================================
# COMPUTE PROJECTIONS
# ============================================================================
proj = pipeline_df.copy()
proj["CLOSE_RATE"] = proj["STAGE"].map(close_rates)
proj["WEIGHTED_ACV_K"] = proj["ACV_K"] * proj["CLOSE_RATE"]

total_pipeline = proj["ACV_K"].sum()
total_weighted = proj["WEIGHTED_ACV_K"].sum()
blended_rate = total_weighted / total_pipeline if total_pipeline > 0 else 0
num_opps = len(proj)
num_accounts = proj["ACCOUNT_NAME"].nunique()


# ============================================================================
# KPI ROW
# ============================================================================
kpi_cols = st.columns(5)
kpi_cols[0].metric("Total Pipeline (ACV)", f"${total_pipeline:,.0f}K")
kpi_cols[1].metric("Weighted Revenue", f"${total_weighted:,.0f}K")
kpi_cols[2].metric("Blended Close Rate", f"{blended_rate:.0%}")
kpi_cols[3].metric("Opportunities", f"{num_opps}")
kpi_cols[4].metric("Accounts", f"{num_accounts}")


# ============================================================================
# CHARTS ROW 1: By AE and By Stage
# ============================================================================
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Weighted Revenue by AE**")
    by_ae = (
        proj.groupby("AE_NAME", as_index=False)["WEIGHTED_ACV_K"]
        .sum()
        .sort_values("WEIGHTED_ACV_K", ascending=False)
    )
    chart_ae = (
        alt.Chart(by_ae)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("AE_NAME:N", sort="-y", title="Account Executive"),
            y=alt.Y("WEIGHTED_ACV_K:Q", title="Weighted Total ACV ($K)"),
            color=alt.Color("AE_NAME:N", legend=None),
            tooltip=[
                alt.Tooltip("AE_NAME:N", title="AE"),
                alt.Tooltip("WEIGHTED_ACV_K:Q", title="Weighted Total ACV ($K)", format=",.0f"),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(chart_ae, use_container_width=True)

with col_right:
    st.markdown("**Weighted Revenue by Stage**")
    by_stage = proj.groupby("STAGE", as_index=False)["WEIGHTED_ACV_K"].sum()
    by_stage["STAGE"] = pd.Categorical(
        by_stage["STAGE"], categories=STAGES, ordered=True
    )
    by_stage = by_stage.sort_values("STAGE")
    chart_stage = (
        alt.Chart(by_stage)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("STAGE:N", sort=STAGES, title="Stage"),
            y=alt.Y("WEIGHTED_ACV_K:Q", title="Weighted Total ACV ($K)"),
            color=alt.Color("STAGE:N", legend=None),
            tooltip=[
                alt.Tooltip("STAGE:N", title="Stage"),
                alt.Tooltip("WEIGHTED_ACV_K:Q", title="Weighted Total ACV ($K)", format=",.0f"),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(chart_stage, use_container_width=True)


# ============================================================================
# CHART ROW 2: Stacked composition
# ============================================================================
st.markdown("**Pipeline Composition: Weighted Total ACV by AE & Stage**")
stacked = proj[proj["WEIGHTED_ACV_K"] > 0].copy()
stacked["STAGE"] = pd.Categorical(
    stacked["STAGE"], categories=STAGES, ordered=True
)
chart_stacked = (
    alt.Chart(stacked)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("AE_NAME:N", sort="-y", title="Account Executive"),
        y=alt.Y("WEIGHTED_ACV_K:Q", title="Weighted Total ACV ($K)", stack="zero"),
        color=alt.Color(
            "STAGE:N",
            sort=STAGES,
            title="Stage",
            scale=alt.Scale(scheme="tableau10"),
        ),
        tooltip=[
            alt.Tooltip("AE_NAME:N", title="AE"),
            alt.Tooltip("STAGE:N", title="Stage"),
            alt.Tooltip("ACCOUNT_NAME:N", title="Account"),
            alt.Tooltip("ACV_K:Q", title="Raw Total ACV ($K)", format=",.0f"),
            alt.Tooltip("WEIGHTED_ACV_K:Q", title="Weighted ($K)", format=",.0f"),
        ],
    )
    .properties(height=400)
)
st.altair_chart(chart_stacked, use_container_width=True)


# ============================================================================
# DETAILED TABLE
# ============================================================================
st.markdown("---")
st.markdown("**Detailed Pipeline & Projections**")

ae_filter = st.multiselect(
    "Filter by AE",
    options=sorted(proj["AE_NAME"].unique()),
    default=sorted(proj["AE_NAME"].unique()),
    key="ae_filter",
)
filtered = proj[proj["AE_NAME"].isin(ae_filter)].copy()
filtered["CLOSE_RATE_FMT"] = filtered["CLOSE_RATE"].map(lambda x: f"{x:.0%}" if pd.notna(x) else "")
filtered["ACV_K_FMT"] = filtered["ACV_K"].map(lambda x: f"${x:,.0f}K")
filtered["WEIGHTED_FMT"] = filtered["WEIGHTED_ACV_K"].map(lambda x: f"${x:,.0f}K" if pd.notna(x) else "")

display_df = filtered[
    [
        "ACCOUNT_NAME", "OPPORTUNITY_NAME", "AE_NAME", "SE_NAME", "STAGE",
        "ACV_K_FMT", "CLOSE_RATE_FMT", "WEIGHTED_FMT",
        "CLOSE_DATE", "COMPETITORS",
    ]
].rename(columns={
    "ACCOUNT_NAME": "Account",
    "OPPORTUNITY_NAME": "Opportunity",
    "AE_NAME": "AE",
    "SE_NAME": "SE",
    "STAGE": "Stage",
    "ACV_K_FMT": "ACV ($K)",
    "CLOSE_RATE_FMT": "Close Rate",
    "WEIGHTED_FMT": "Weighted ($K)",
    "CLOSE_DATE": "Close Date",
    "COMPETITORS": "Competitors",
}).reset_index(drop=True)

st.dataframe(display_df, use_container_width=True)


# ============================================================================
# STAGE CONVERSION REFERENCE
# ============================================================================
with st.sidebar:
    st.markdown("---")
    st.header("Stage Reference")
    for _, row in cr_df.iterrows():
        st.markdown(f"**{row['STAGE']}**  \n_{row['DESCRIPTION']}_")
