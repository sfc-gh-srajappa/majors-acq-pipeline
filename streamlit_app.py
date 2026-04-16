import streamlit as st
import pandas as pd
import altair as alt

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    IS_SIS = True
except:
    IS_SIS = False

if not IS_SIS:
    st.set_page_config(
        page_title="NE Majors Competitive Intel",
        page_icon=":material/target:",
        layout="wide",
    )

ALL_COMPETITORS = [
    "Databricks", "AWS Redshift", "Google BigQuery",
    "Microsoft Fabric", "Azure Synapse",
    "Oracle", "Teradata", "Hadoop", "Vertica", "SAP",
    "C3 AI", "Palantir",
    "Snowflake (existing)", "Greenfield", "Proprietary", "Unknown", "Other",
]

COMPETITOR_DOTS = {
    "Databricks": "\U0001f534", "AWS Redshift": "\U0001f7e1", "Google BigQuery": "\U0001f7e2",
    "Microsoft Fabric": "\U0001f535", "Azure Synapse": "\U0001f535",
    "Oracle": "\U0001f7e0", "Teradata": "\U0001f7e0", "Hadoop": "\U0001f7e0", "Vertica": "\U0001f7e0",
    "SAP": "\U0001f7e0", "C3 AI": "\U0001f7e0", "Palantir": "\U0001f7e0",
    "Greenfield": "\u26aa", "Proprietary": "\u26aa",
    "Snowflake (existing)": "\U0001f537", "Unknown": "\u26aa", "Other": "\u26aa",
}

color_map = {
    "Databricks": "#FF4B4B", "AWS Redshift": "#FFD700", "Google BigQuery": "#34A853",
    "Microsoft Fabric": "#4285F4", "Azure Synapse": "#4285F4",
    "Oracle": "#FF8C00", "Teradata": "#FF8C00", "Hadoop": "#FF8C00",
    "Vertica": "#FF8C00", "SAP": "#FF8C00", "C3 AI": "#FF8C00", "Palantir": "#FF8C00",
    "Greenfield": "#CCCCCC", "Proprietary": "#CCCCCC", "Snowflake (existing)": "#29B5E8",
    "Unknown": "#999999", "Other": "#999999",
}

EDITS_TABLE = "SNOWPUBLIC.STREAMLIT.NE_COMPETITIVE_INTEL_EDITS"
TERRITORY_TABLE = "SNOWPUBLIC.STREAMLIT.T_TERRITORY_ACCOUNTS"

BATTLE_CARDS = {
    "Databricks": {
        "lead": "Governance, ease of use, TCO",
        "tips": [
            "Snowflake unified SQL platform vs notebook-first approach \u2014 80% of workloads don't need Spark",
            "Native governance & RBAC maturity vs Unity Catalog still evolving; ask about row-level security & dynamic masking",
            "Zero-copy cloning & time travel create instant dev/test environments \u2014 ask how they do this today",
            "Snowflake Marketplace for 3rd-party data enrichment \u2014 Databricks Marketplace is nascent",
            "Near-zero admin vs cluster sizing, autoscaling tuning, and DBU cost management overhead",
        ]
    },
    "AWS Redshift": {
        "lead": "Multi-cloud flexibility, separation of storage/compute, zero admin",
        "tips": [
            "Cross-cloud data sharing across AWS, Azure, GCP vs locked into AWS ecosystem",
            "True separation of storage and compute with instant scaling \u2014 Redshift Serverless is limited, RA3 still needs cluster mgmt",
            "Near-zero maintenance vs vacuum, analyze, sort key optimization, and WLM tuning",
            "Ask about Redshift pain points: concurrency scaling costs, query queuing, cluster resize downtime",
            "Snowflake Marketplace + data sharing vs Redshift's limited data sharing capabilities",
        ]
    },
    "Google BigQuery": {
        "lead": "Multi-cloud, workload isolation, governance",
        "tips": [
            "Not locked to GCP \u2014 Snowflake runs across all three clouds with unified experience",
            "Dedicated virtual warehouses for workload isolation vs shared slot pool contention",
            "Stronger RBAC, row-level security, dynamic data masking vs BigQuery's IAM-based model",
            "Snowpark for data engineering in Python/Java/Scala vs BigQuery's limited procedural capabilities",
            "Predictable per-second compute billing vs BigQuery on-demand slot pricing surprises",
        ]
    },
    "Microsoft Fabric": {
        "lead": "Platform maturity, performance consistency, ecosystem",
        "tips": [
            "Fabric is still early \u2014 many features in preview; Snowflake has 10+ years of production hardening",
            "Snowflake Marketplace and data sharing dwarfs Fabric OneLake sharing capabilities",
            "Consistent performance at any scale vs Fabric capacity unit throttling and SKU limitations",
            "True multi-cloud vs Azure-only lock-in with Fabric",
            "Many customers finding gaps between Fabric promises and reality \u2014 ask what's actually in production",
        ]
    },
    "Azure Synapse": {
        "lead": "Simplicity, performance, total cost of ownership",
        "tips": [
            "Snowflake auto-optimization vs Synapse distribution keys, indexing, materialized views tuning",
            "Instant elastic scaling vs Synapse dedicated pool pause/resume cycles",
            "Cross-cloud portability vs Azure lock-in; data sharing across any cloud",
            "Synapse Serverless SQL pool has significant performance and format limitations",
            "Many Azure shops running both Synapse AND Databricks \u2014 consolidate on Snowflake",
        ]
    },
    "Oracle": {
        "lead": "Cloud-native modernization, simplicity, no vendor lock-in",
        "tips": [
            "Cloud-native architecture built for the cloud vs Oracle's lift-and-shift Autonomous DB",
            "Transparent consumption pricing vs Oracle's complex licensing and audit risks",
            "Dramatically simpler admin \u2014 no DBA army for tuning, patching, upgrades",
            "Open ecosystem with any BI/ETL tool vs Oracle's push toward all-Oracle stack",
            "Proven Oracle-to-Snowflake migration patterns \u2014 Snowflake can ingest Oracle sources easily",
        ]
    },
    "Teradata": {
        "lead": "Cloud modernization, massive TCO savings, elastic scaling",
        "tips": [
            "Proven Teradata-to-Snowflake migration path \u2014 many enterprise references available",
            "60-80% TCO reduction commonly seen vs Teradata on-prem or Vantage cloud",
            "Elastic scaling for variable workloads vs fixed Teradata capacity (pay for peak forever)",
            "No more capacity planning, hardware refreshes, or expensive Teradata consulting",
            "Snowflake data sharing and marketplace vs Teradata's isolated architecture",
        ]
    },
    "Hadoop": {
        "lead": "Massive simplification, cost reduction, eliminate cluster management",
        "tips": [
            "Eliminate Hadoop cluster management, tuning, and admin teams entirely",
            "SQL-first approach \u2014 no more MapReduce, Hive, or Spark complexity for analytics",
            "Instant elasticity vs fixed cluster capacity; pay only for what you use",
            "Structured + semi-structured data in one platform vs Hadoop's fragmented tool zoo",
            "Most Hadoop workloads migrate to Snowflake in weeks, not months",
        ]
    },
    "SAP": {
        "lead": "Analytics modernization, open ecosystem, performance",
        "tips": [
            "Modernize from SAP BW/HANA to cloud-native analytics \u2014 keep SAP as ERP source",
            "10-100x query performance improvement vs SAP BW for analytics workloads",
            "Open ecosystem \u2014 any BI tool vs SAP's push toward SAP Analytics Cloud only",
            "Data sharing enables cross-org analytics that SAP BW can't match",
            "Proven SAP extraction via Fivetran, Matillion, or native connectors",
        ]
    },
}


HARDCODED_DM = "Joe DiCrecchio"

TOP_REVENUE_ACCOUNTS = [
    {"Account": "StoneX", "Revenue": 99.9e9, "Fortune": "#42"},
    {"Account": "MetLife", "Revenue": 71.0e9, "Fortune": "#60"},
    {"Account": "Merck", "Revenue": 64.2e9, "Fortune": "#65"},
    {"Account": "Ahold U.S.A.", "Revenue": 58.5e9, "Fortune": ""},
    {"Account": "PriceWaterhouseCoopers (PwC)", "Revenue": 55.4e9, "Fortune": ""},
    {"Account": "Bristol Myers Squibb", "Revenue": 48.3e9, "Fortune": "#94"},
    {"Account": "MassMutual", "Revenue": 41.4e9, "Fortune": "#102"},
    {"Account": "GE Vernova", "Revenue": 34.0e9, "Fortune": "#130"},
    {"Account": "Takeda Pharmaceuticals", "Revenue": 29.4e9, "Fortune": ""},
    {"Account": "Marsh McLennan", "Revenue": 24.4e9, "Fortune": "#175"},
]


def load_territory():
    sql = f"""
    WITH territory_base AS (
        SELECT t.SALESFORCE_ACCOUNT_ID, t.SFDC_ACCOUNT_NAME, t.ACCOUNT_EXECUTIVE,
               t.DM, t.SALES_ENGINEER, t.SE_MANAGER, a.PARENT_ID
        FROM {TERRITORY_TABLE} t
        JOIN FIVETRAN.SALESFORCE.ACCOUNT a ON t.SALESFORCE_ACCOUNT_ID = a.ID
        WHERE t.DM = '{HARDCODED_DM}'
    ),
    child_of_territory AS (
        SELECT tb.SALESFORCE_ACCOUNT_ID
        FROM territory_base tb
        INNER JOIN territory_base tp ON tb.PARENT_ID = tp.SALESFORCE_ACCOUNT_ID
    ),
    sibling_ranked AS (
        SELECT tb.SALESFORCE_ACCOUNT_ID,
               ROW_NUMBER() OVER (
                   PARTITION BY tb.PARENT_ID, tb.ACCOUNT_EXECUTIVE
                   ORDER BY JAROWINKLER_SIMILARITY(LOWER(tb.SFDC_ACCOUNT_NAME), LOWER(p.NAME)) DESC
               ) AS rn
        FROM territory_base tb
        JOIN FIVETRAN.SALESFORCE.ACCOUNT p ON tb.PARENT_ID = p.ID
        WHERE tb.PARENT_ID IS NOT NULL
          AND tb.SALESFORCE_ACCOUNT_ID NOT IN (SELECT SALESFORCE_ACCOUNT_ID FROM child_of_territory)
          AND EXISTS (
              SELECT 1 FROM territory_base tb2
              WHERE tb2.PARENT_ID = tb.PARENT_ID
                AND tb2.SALESFORCE_ACCOUNT_ID != tb.SALESFORCE_ACCOUNT_ID
                AND tb2.ACCOUNT_EXECUTIVE = tb.ACCOUNT_EXECUTIVE
          )
    ),
    exclude_siblings AS (
        SELECT SALESFORCE_ACCOUNT_ID FROM sibling_ranked WHERE rn > 1
    )
    SELECT DISTINCT
        tb.SALESFORCE_ACCOUNT_ID,
        tb.SFDC_ACCOUNT_NAME AS ACCOUNT_NAME,
        tb.ACCOUNT_EXECUTIVE AS AE,
        tb.DM,
        tb.SALES_ENGINEER AS SE,
        tb.SE_MANAGER
    FROM territory_base tb
    WHERE tb.SALESFORCE_ACCOUNT_ID NOT IN (SELECT SALESFORCE_ACCOUNT_ID FROM child_of_territory)
      AND tb.SALESFORCE_ACCOUNT_ID NOT IN (SELECT SALESFORCE_ACCOUNT_ID FROM exclude_siblings)
    ORDER BY tb.ACCOUNT_EXECUTIVE, tb.SFDC_ACCOUNT_NAME
    """
    if IS_SIS:
        return session.sql(sql).to_pandas()
    else:
        import snowflake.connector, os
        conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)


PIPELINE_TABLE = "SNOWPUBLIC.STREAMLIT.NE_COMPETITIVE_INTEL_PIPELINE"


def _run_sql(sql):
    if IS_SIS:
        return session.sql(sql).to_pandas()
    else:
        import snowflake.connector, os
        conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)


def load_pipeline(account_ids):
    ids = ",".join([f"'{a}'" for a in account_ids])
    sql = f"""
    SELECT
        p.SALESFORCE_ACCOUNT_ID,
        p.OPPORTUNITY_NAME,
        p.FORECAST_ACV,
        p.FORECAST_STATUS,
        p.STAGE_NAME
    FROM {PIPELINE_TABLE} p
    WHERE p.SALESFORCE_ACCOUNT_ID IN ({ids})
    """
    try:
        return _run_sql(sql)
    except Exception as e:
        st.toast(f"Could not load pipeline: {e}")
        return pd.DataFrame()


def _to_bool(val):
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).upper() in ("TRUE", "1", "YES")


def load_saved_edits():
    sql = f"SELECT * FROM {EDITS_TABLE}"
    try:
        if IS_SIS:
            rows = session.sql(sql).collect()
            edits = {}
            for r in rows:
                edits[r["ACCOUNT_NAME"]] = {
                    "Competitor": r["COMPETITOR"] or "",
                    "Engaged": _to_bool(r["ENGAGED"]),
                    "Est. Spend": r["EST_SPEND"] or "",
                    "Confirmed Spend": _to_bool(r["CONFIRMED_SPEND"]),
                    "Notes": r["NOTES"] or "",
                }
            return edits
        else:
            import snowflake.connector, os
            conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            edits = {}
            for row in cur.fetchall():
                r = dict(zip(cols, row))
                edits[r["ACCOUNT_NAME"]] = {
                    "Competitor": r["COMPETITOR"] or "",
                    "Engaged": _to_bool(r["ENGAGED"]),
                    "Est. Spend": r["EST_SPEND"] or "",
                    "Confirmed Spend": _to_bool(r["CONFIRMED_SPEND"]),
                    "Notes": r["NOTES"] or "",
                }
            cur.close()
            conn.close()
            return edits
    except Exception as e:
        st.toast(f"Could not load saved edits: {e}")
        return {}


def save_edit(acct, overrides):
    comp = str(overrides.get("Competitor", "")).replace("'", "''")
    engaged = overrides.get("Engaged", False)
    est_spend = str(overrides.get("Est. Spend", "")).replace("'", "''")
    confirmed = overrides.get("Confirmed Spend", False)
    notes = str(overrides.get("Notes", "")).replace("'", "''")
    acct_esc = acct.replace("'", "''")
    sql = f"""MERGE INTO {EDITS_TABLE} t
USING (SELECT '{acct_esc}' AS ACCOUNT_NAME) s
ON t.ACCOUNT_NAME = s.ACCOUNT_NAME
WHEN MATCHED THEN UPDATE SET
    COMPETITOR = '{comp}',
    ENGAGED = {str(engaged).upper()},
    EST_SPEND = '{est_spend}',
    CONFIRMED_SPEND = {str(confirmed).upper()},
    NOTES = '{notes}',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (ACCOUNT_NAME, COMPETITOR, ENGAGED, EST_SPEND, CONFIRMED_SPEND, NOTES)
    VALUES ('{acct_esc}', '{comp}', {str(engaged).upper()}, '{est_spend}', {str(confirmed).upper()}, '{notes}')"""
    try:
        if IS_SIS:
            session.sql(sql).collect()
        else:
            import snowflake.connector, os
            conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
            conn.cursor().execute(sql)
            conn.close()
    except:
        pass


territory_df = load_territory()

if "editor_overrides" not in st.session_state:
    st.session_state["editor_overrides"] = load_saved_edits()

st.title("NE Majors Competitive Intel")
st.caption("FY27 | NE Majors Acquisition")


with st.sidebar:
    st.header(":material/filter_list: Filters")
    st.markdown(f"**DM:** {HARDCODED_DM}")

    ae_options = sorted(territory_df["AE"].unique())
    selected_ae = st.selectbox(
        "Account Executive",
        options=["All"] + ae_options,
        format_func=lambda x: f"{x} ({len(territory_df[territory_df['AE'] == x])} accounts)" if x != "All" else "All AE's"
    )

    if selected_ae != "All":
        ae_rows = territory_df[territory_df["AE"] == selected_ae]
        se_name = ae_rows["SE"].iloc[0] if not ae_rows.empty else ""
        se_mgr = ae_rows["SE_MANAGER"].iloc[0] if not ae_rows.empty else ""
        st.markdown(f"**SE:** {se_name}")
        st.markdown(f"**SE Manager:** {se_mgr}")

    engaged_only = st.checkbox("Engaged only")

    st.divider()
    sidebar_accounts = territory_df if selected_ae == "All" else territory_df[territory_df["AE"] == selected_ae]
    with st.expander(f"All Accounts ({len(sidebar_accounts)})"):
        for acct in sorted(sidebar_accounts["ACCOUNT_NAME"].unique()):
            st.markdown(f"- {acct}")

if selected_ae == "All":
    filtered = territory_df.copy()
else:
    filtered = territory_df[territory_df["AE"] == selected_ae].copy()

filtered["Competitor"] = "Unknown"
filtered["Engaged"] = False
filtered["Est. Spend"] = ""
filtered["Confirmed Spend"] = False
filtered["Notes"] = ""

overrides_dict = st.session_state.get("editor_overrides", {})
for acct, overrides in overrides_dict.items():
    mask = filtered["ACCOUNT_NAME"] == acct
    if mask.any():
        idx = filtered.index[mask]
        if overrides.get("Competitor"):
            filtered.loc[idx, "Competitor"] = str(overrides["Competitor"])
        if "Engaged" in overrides:
            filtered.loc[idx, "Engaged"] = bool(overrides["Engaged"])
        if overrides.get("Est. Spend"):
            filtered.loc[idx, "Est. Spend"] = str(overrides["Est. Spend"])
        if "Confirmed Spend" in overrides:
            filtered.loc[idx, "Confirmed Spend"] = bool(overrides["Confirmed Spend"])
        if overrides.get("Notes"):
            filtered.loc[idx, "Notes"] = str(overrides["Notes"])

if engaged_only:
    filtered = filtered[filtered["Engaged"]]

filtered = filtered.reset_index(drop=True)

acct_ids = filtered["SALESFORCE_ACCOUNT_ID"].dropna().unique().tolist()
pipeline_df = load_pipeline(acct_ids) if acct_ids else pd.DataFrame()

tab_overview, tab_competitors, tab_pipeline, tab_targets, tab_playbook, tab_editor = st.tabs([
    "Overview",
    "Competitor Breakdown",
    "Pipeline Detail",
    "Priority Targets",
    "Displacement Playbook",
    "Account Editor",
])

with tab_overview:
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Accounts", len(filtered))
    engaged_count = int(filtered["Engaged"].sum())
    engaged_pct = int((engaged_count / max(len(filtered), 1)) * 100)
    gap_count = len(filtered) - engaged_count
    col_m2.metric("Engaged", f"{engaged_count} ({engaged_pct}%)")
    col_m3.metric("Engagement Gap", gap_count, delta=f"{100 - engaged_pct}% not engaged", delta_color="inverse")
    known_competitors = filtered[~filtered["Competitor"].isin(["Unknown", ""])]["Competitor"].nunique()
    col_m4.metric("Competitors Identified", known_competitors)

    col_m5, col_m6, col_m7, col_m8 = st.columns(4)
    intel_pct = int((filtered[~filtered["Competitor"].isin(["Unknown", ""])].shape[0] / max(len(filtered), 1)) * 100)
    col_m5.metric("Intel Coverage", f"{intel_pct}%")
    not_engaged_known = filtered[(~filtered["Competitor"].isin(["Unknown", ""])) & (~filtered["Engaged"])]
    col_m6.metric("Needs Engagement", len(not_engaged_known))
    unknown_count = filtered[filtered["Competitor"].isin(["Unknown", ""])].shape[0]
    col_m7.metric("Needs Intel", unknown_count)
    total_pipeline = pipeline_df["FORECAST_ACV"].sum() if not pipeline_df.empty else 0
    col_m8.metric("Total Pipeline", f"${total_pipeline/1e6:.1f}M" if total_pipeline >= 1e6 else f"${total_pipeline/1e3:.0f}K")

    st.divider()

    st.subheader("Top 10 Accounts by Revenue")
    st.caption("Green = engaged  |  Red = gap  |  Competitor intel shown")
    rev_rows = []
    for i, entry in enumerate(TOP_REVENUE_ACCOUNTS, 1):
        acct_name = entry["Account"]
        match = filtered[filtered["ACCOUNT_NAME"].str.contains(acct_name.split("(")[0].strip(), case=False, na=False)]
        if match.empty:
            match = filtered[filtered["ACCOUNT_NAME"].str.contains(acct_name.split()[0], case=False, na=False)]
        is_engaged = bool(match.iloc[0]["Engaged"]) if not match.empty else False
        competitor = str(match.iloc[0]["Competitor"]) if not match.empty else "Unknown"
        fortune = entry["Fortune"] if entry["Fortune"] else "\u2014"
        rev_b = entry["Revenue"] / 1e9
        rev_rows.append({
            "rank": i,
            "account": acct_name,
            "revenue": f"${rev_b:.1f}B",
            "revenue_raw": entry["Revenue"],
            "fortune": fortune,
            "competitor": competitor,
            "engaged": is_engaged,
        })

    total_rev = sum(r["revenue_raw"] for r in rev_rows)
    engaged_rev = sum(r["revenue_raw"] for r in rev_rows if r["engaged"])
    gap_rev = total_rev - engaged_rev
    top_competitors = pd.Series([r["competitor"] for r in rev_rows]).value_counts()
    top_comp_name = top_competitors.index[0] if not top_competitors.empty else "Unknown"
    top_comp_count = int(top_competitors.iloc[0]) if not top_competitors.empty else 0

    rev_df = pd.DataFrame(rev_rows)[["rank", "account", "revenue", "fortune", "competitor", "engaged"]]
    rev_df.columns = ["#", "Account", "Revenue", "Fortune", "Competitor", "_engaged"]
    rev_df["Account"] = rev_df.apply(
        lambda r: f"\U0001f7e2 {r['Account']}" if r["_engaged"] else f"\U0001f534 {r['Account']}", axis=1
    )
    rev_df["Competitor"] = rev_df.apply(
        lambda r: f"{COMPETITOR_DOTS.get(r['Competitor'], chr(9898))} {r['Competitor']}", axis=1
    )
    st.dataframe(
        rev_df[["#", "Account", "Revenue", "Fortune", "Competitor"]],
        hide_index=True, use_container_width=True,
    )
    st.info(f"Total: ${total_rev/1e9:.1f}B  |  Engaged: ${engaged_rev/1e9:.1f}B  |  Gap: ${gap_rev/1e9:.1f}B  |  {top_comp_count} of 10 facing {top_comp_name}")

    st.divider()

    if selected_ae == "All":
        all_ae_se = filtered[["AE", "SE"]].drop_duplicates()
        if not pipeline_df.empty:
            pipe_with_ae = pipeline_df.merge(
                filtered[["SALESFORCE_ACCOUNT_ID", "AE", "SE"]].drop_duplicates(),
                on="SALESFORCE_ACCOUNT_ID", how="inner"
            )
            ae_pipe = pipe_with_ae.groupby("AE").agg(
                Pipeline=("FORECAST_ACV", "sum"),
                SE=("SE", "first"),
                Opps=("FORECAST_ACV", "count")
            ).reset_index()
        else:
            ae_pipe = pd.DataFrame(columns=["AE", "Pipeline", "SE", "Opps"])
        missing_aes = all_ae_se[~all_ae_se["AE"].isin(ae_pipe["AE"])]
        if not missing_aes.empty:
            zero_rows = pd.DataFrame({
                "AE": missing_aes["AE"].values,
                "Pipeline": 0.0,
                "SE": missing_aes["SE"].values,
                "Opps": 0,
            })
            ae_pipe = pd.concat([ae_pipe, zero_rows], ignore_index=True)
        st.subheader("FY27 Open Forecasted Pipeline by AE")
        ae_pipe["Pipeline_Display"] = ae_pipe["Pipeline"].apply(lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else (f"${v/1e3:.0f}K" if v > 0 else "$0"))
        ae_sort = ae_pipe.sort_values("Pipeline", ascending=False)["AE"].tolist()
        bars = alt.Chart(ae_pipe).mark_bar(cornerRadius=4, color="#29B5E8").encode(
            x=alt.X("AE:N", sort=ae_sort, title=None, axis=alt.Axis(labelAngle=-25)),
            y=alt.Y("Pipeline:Q", title="Forecast ACV ($)", axis=alt.Axis(format="$,.0f")),
            tooltip=[alt.Tooltip("AE:N"), alt.Tooltip("SE:N"), alt.Tooltip("Pipeline:Q", format="$,.0f"), alt.Tooltip("Opps:Q", title="Opportunities")]
        )
        text = alt.Chart(ae_pipe).mark_text(dy=-10, fontSize=13, fontWeight="bold", color="white").encode(
            x=alt.X("AE:N", sort=ae_sort),
            y=alt.Y("Pipeline:Q"),
            text="Pipeline_Display:N"
        )
        st.altair_chart((bars + text).properties(height=350), use_container_width=True)

    if not pipeline_df.empty:
        st.subheader("Top 10 Accounts by Pipeline")
        acct_pipe = pipeline_df.groupby("SALESFORCE_ACCOUNT_ID").agg(
            Pipeline=("FORECAST_ACV", "sum"),
            Opps=("FORECAST_ACV", "count")
        ).reset_index()
        acct_pipe = acct_pipe.merge(
            filtered[["SALESFORCE_ACCOUNT_ID", "ACCOUNT_NAME", "AE", "Competitor", "Engaged"]].drop_duplicates(),
            on="SALESFORCE_ACCOUNT_ID", how="inner"
        )
        top10 = acct_pipe.sort_values("Pipeline", ascending=False).head(10)
        top10["Status"] = top10["Engaged"].apply(lambda x: "Engaged" if x else "Not Engaged")
        top10["Pipeline_Display"] = top10["Pipeline"].apply(lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K")
        acct_sort = top10.sort_values("Pipeline", ascending=True)["ACCOUNT_NAME"].tolist()
        bars = alt.Chart(top10).mark_bar(cornerRadius=4).encode(
            y=alt.Y("ACCOUNT_NAME:N", sort=acct_sort, title=None),
            x=alt.X("Pipeline:Q", title="Forecast ACV ($)", axis=alt.Axis(format="$,.0f")),
            color=alt.Color("Status:N", scale=alt.Scale(domain=["Engaged", "Not Engaged"], range=["#34A853", "#FF4B4B"]), title="Status"),
            tooltip=[
                alt.Tooltip("ACCOUNT_NAME:N", title="Account"),
                alt.Tooltip("AE:N"),
                alt.Tooltip("Pipeline:Q", format="$,.0f"),
                alt.Tooltip("Competitor:N"),
                alt.Tooltip("Status:N")
            ]
        )
        text = alt.Chart(top10).mark_text(dx=5, align="left", fontSize=12, fontWeight="bold").encode(
            y=alt.Y("ACCOUNT_NAME:N", sort=acct_sort),
            x=alt.X("Pipeline:Q"),
            text="Pipeline_Display:N"
        )
        st.altair_chart((bars + text).properties(height=350), use_container_width=True)

    if selected_ae == "All":
        st.divider()
        st.subheader("SE Readiness Scorecard")
        se_scores = []
        for ae_name in filtered["AE"].unique():
            ae_accts = filtered[filtered["AE"] == ae_name]
            se_name = ae_accts["SE"].iloc[0] if not ae_accts.empty else ""
            total = len(ae_accts)
            known = ae_accts[~ae_accts["Competitor"].isin(["Unknown", ""])].shape[0]
            engaged = int(ae_accts["Engaged"].sum())
            ae_ids = ae_accts["SALESFORCE_ACCOUNT_ID"].dropna().unique().tolist()
            ae_pipeline = 0
            ae_opps = 0
            if not pipeline_df.empty and ae_ids:
                ae_pipe_rows = pipeline_df[pipeline_df["SALESFORCE_ACCOUNT_ID"].isin(ae_ids)]
                ae_pipeline = ae_pipe_rows["FORECAST_ACV"].sum()
                ae_opps = len(ae_pipe_rows)
            intel_score = int((known / max(total, 1)) * 100)
            engage_score = int((engaged / max(total, 1)) * 100)
            readiness = int((intel_score * 0.3 + engage_score * 0.3 + min(ae_pipeline / 500000, 1) * 40))
            se_scores.append({
                "AE": ae_name,
                "SE": se_name,
                "Accounts": total,
                "Intel %": f"{intel_score}%",
                "Engaged %": f"{engage_score}%",
                "Pipeline": ae_pipeline,
                "Opps": ae_opps,
                "Readiness": f"{readiness}/100",
            })
        score_df = pd.DataFrame(se_scores).sort_values("Pipeline", ascending=False)
        score_df["Pipeline"] = score_df["Pipeline"].apply(lambda v: f"${v:,.0f}")
        st.dataframe(
            score_df,
            hide_index=True, use_container_width=True,
        )
        st.caption("Readiness = 30% intel coverage + 30% engagement rate + 40% pipeline ($500K = max)")


with tab_competitors:
    st.subheader("Competitor Landscape")
    comp_counts = filtered.groupby("Competitor").size().reset_index(name="Accounts")
    comp_counts = comp_counts.sort_values("Accounts", ascending=False)

    col_chart, col_breakdown = st.columns([3, 1])

    with col_chart:
        comp_sort = comp_counts["Competitor"].tolist()
        comp_bar = alt.Chart(comp_counts).mark_bar(cornerRadius=4).encode(
            x=alt.X("Competitor:N", sort=comp_sort, title=None, axis=alt.Axis(labelAngle=-25)),
            y=alt.Y("Accounts:Q", title="# Accounts"),
            color=alt.Color("Competitor:N", scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())), legend=None),
            tooltip=[alt.Tooltip("Competitor:N"), alt.Tooltip("Accounts:Q")]
        )
        comp_text = alt.Chart(comp_counts).mark_text(dy=-10, fontSize=13, fontWeight="bold").encode(
            x=alt.X("Competitor:N", sort=comp_sort),
            y=alt.Y("Accounts:Q"),
            text="Accounts:Q"
        )
        st.altair_chart((comp_bar + comp_text).properties(height=300), use_container_width=True)

    with col_breakdown:
        for _, row in comp_counts.iterrows():
            dot = COMPETITOR_DOTS.get(row["Competitor"], "\u26aa")
            st.markdown(f"{dot} **{row['Competitor']}**: {row['Accounts']}")

    st.divider()
    st.subheader("Competitor Breakdown by Account")
    comp_detail = filtered[["AE", "SE", "ACCOUNT_NAME", "Competitor", "Engaged"]].copy()
    comp_detail["Competitor"] = comp_detail["Competitor"].apply(lambda c: f"{COMPETITOR_DOTS.get(c, chr(9898))} {c}")
    comp_detail = comp_detail.sort_values(["Competitor", "ACCOUNT_NAME"])
    st.dataframe(
        comp_detail.rename(columns={"ACCOUNT_NAME": "Account"}),
        hide_index=True, use_container_width=True, height=500,
    )

    if selected_ae == "All":
        st.divider()
        st.subheader("Competitor Distribution by AE")
        ae_comp = filtered.groupby(["AE", "Competitor"]).size().reset_index(name="Accounts")
        ae_comp_chart = alt.Chart(ae_comp).mark_bar(cornerRadius=2).encode(
            x=alt.X("AE:N", title=None, axis=alt.Axis(labelAngle=-25)),
            y=alt.Y("Accounts:Q", title="# Accounts", stack="zero"),
            color=alt.Color("Competitor:N", scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())), title="Competitor"),
            tooltip=[alt.Tooltip("AE:N"), alt.Tooltip("Competitor:N"), alt.Tooltip("Accounts:Q")]
        )
        st.altair_chart(ae_comp_chart.properties(height=350), use_container_width=True)


with tab_pipeline:
    if pipeline_df.empty:
        st.info("No pipeline data available for the selected accounts.")
    else:
        st.subheader("FY27 Pipeline Detail")
        pipe_detail = pipeline_df.merge(
            filtered[["SALESFORCE_ACCOUNT_ID", "ACCOUNT_NAME", "AE", "SE", "Competitor"]].drop_duplicates(),
            on="SALESFORCE_ACCOUNT_ID", how="inner"
        )
        col_p1, col_p2, col_p3 = st.columns(3)
        col_p1.metric("Total Pipeline", f"${pipe_detail['FORECAST_ACV'].sum()/1e6:.1f}M" if pipe_detail['FORECAST_ACV'].sum() >= 1e6 else f"${pipe_detail['FORECAST_ACV'].sum()/1e3:.0f}K")
        col_p2.metric("Opportunities", len(pipe_detail))
        col_p3.metric("Avg Deal Size", f"${pipe_detail['FORECAST_ACV'].mean()/1e3:.0f}K" if len(pipe_detail) > 0 else "$0")

        st.divider()

        st.subheader("Pipeline by Stage")
        stage_agg = pipe_detail.groupby("STAGE_NAME").agg(
            Pipeline=("FORECAST_ACV", "sum"),
            Opps=("FORECAST_ACV", "count")
        ).reset_index().sort_values("Pipeline", ascending=False)
        stage_agg["Pipeline_Display"] = stage_agg["Pipeline"].apply(lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K")
        stage_sort = stage_agg["STAGE_NAME"].tolist()
        stage_bars = alt.Chart(stage_agg).mark_bar(cornerRadius=4, color="#29B5E8").encode(
            x=alt.X("STAGE_NAME:N", sort=stage_sort, title=None, axis=alt.Axis(labelAngle=-25)),
            y=alt.Y("Pipeline:Q", title="Forecast ACV ($)", axis=alt.Axis(format="$,.0f")),
            tooltip=[alt.Tooltip("STAGE_NAME:N", title="Stage"), alt.Tooltip("Pipeline:Q", format="$,.0f"), alt.Tooltip("Opps:Q", title="Opps")]
        )
        stage_text = alt.Chart(stage_agg).mark_text(dy=-10, fontSize=13, fontWeight="bold").encode(
            x=alt.X("STAGE_NAME:N", sort=stage_sort),
            y=alt.Y("Pipeline:Q"),
            text="Pipeline_Display:N"
        )
        st.altair_chart((stage_bars + stage_text).properties(height=300), use_container_width=True)

        st.divider()
        st.subheader("All Opportunities")
        opp_display = pipe_detail[["AE", "ACCOUNT_NAME", "OPPORTUNITY_NAME", "FORECAST_ACV", "STAGE_NAME", "FORECAST_STATUS"]].copy()
        opp_display = opp_display.sort_values("FORECAST_ACV", ascending=False)
        opp_display["FORECAST_ACV"] = opp_display["FORECAST_ACV"].apply(lambda v: f"${v:,.0f}")
        st.dataframe(
            opp_display.rename(columns={
                "ACCOUNT_NAME": "Account",
                "OPPORTUNITY_NAME": "Opportunity",
                "FORECAST_ACV": "Forecast ACV",
                "STAGE_NAME": "Stage",
                "FORECAST_STATUS": "Forecast Status"
            }),
            hide_index=True, use_container_width=True, height=600,
        )


with tab_targets:
    st.subheader("Priority Targets")
    col_p1, col_p2, col_p3 = st.columns(3)

    with col_p1:
        st.markdown("**:material/local_fire_department: Pipeline Gap**")
        st.caption("Engaged + competitor known, but no pipeline yet")
        engaged_known = filtered[(~filtered["Competitor"].isin(["Unknown", ""])) & (filtered["Engaged"])]
        if not pipeline_df.empty:
            pipeline_acct_ids = set(pipeline_df["SALESFORCE_ACCOUNT_ID"].unique())
            gap = engaged_known[~engaged_known["SALESFORCE_ACCOUNT_ID"].isin(pipeline_acct_ids)]
        else:
            gap = engaged_known
        if not gap.empty:
            st.dataframe(
                gap[["AE", "SE", "ACCOUNT_NAME", "Competitor"]].rename(columns={"ACCOUNT_NAME": "Account"}),
                hide_index=True, use_container_width=True, height=400,
            )
        else:
            st.success("No gaps \u2014 all engaged accounts have pipeline!")

    with col_p2:
        st.markdown("**:material/handshake: Needs Engagement**")
        st.caption("Competitor known but not yet engaged")
        needs_engage = filtered[(~filtered["Competitor"].isin(["Unknown", ""])) & (~filtered["Engaged"])]
        if not needs_engage.empty:
            ne_display = needs_engage[["AE", "SE", "ACCOUNT_NAME", "Competitor"]].copy()
            ne_display["Competitor"] = ne_display["Competitor"].apply(lambda c: f"{COMPETITOR_DOTS.get(c, chr(9898))} {c}")
            st.dataframe(
                ne_display.rename(columns={"ACCOUNT_NAME": "Account"}),
                hide_index=True, use_container_width=True, height=400,
            )
        else:
            st.success("All identified accounts are engaged!")

    with col_p3:
        st.markdown("**:material/search: Needs Intel**")
        st.caption("Competitor unknown \u2014 research needed")
        needs_intel = filtered[filtered["Competitor"].isin(["Unknown", ""])]
        if not needs_intel.empty:
            st.dataframe(
                needs_intel[["AE", "SE", "ACCOUNT_NAME"]].rename(columns={"ACCOUNT_NAME": "Account"}),
                hide_index=True, use_container_width=True, height=400,
            )
        else:
            st.success("All accounts have competitor intel!")

    if not pipeline_df.empty:
        st.divider()
        st.subheader("High-Value Unengaged Accounts")
        st.caption("Accounts with pipeline but not marked as engaged")
        unengaged_with_pipe = filtered[~filtered["Engaged"]].copy()
        if not unengaged_with_pipe.empty:
            pipe_by_acct = pipeline_df.groupby("SALESFORCE_ACCOUNT_ID").agg(Pipeline=("FORECAST_ACV", "sum")).reset_index()
            unengaged_pipe = unengaged_with_pipe.merge(pipe_by_acct, on="SALESFORCE_ACCOUNT_ID", how="inner")
            if not unengaged_pipe.empty:
                unengaged_pipe = unengaged_pipe.sort_values("Pipeline", ascending=False)
                unengaged_pipe["Pipeline"] = unengaged_pipe["Pipeline"].apply(lambda v: f"${v:,.0f}")
                st.dataframe(
                    unengaged_pipe[["AE", "SE", "ACCOUNT_NAME", "Competitor", "Pipeline"]].rename(
                        columns={"ACCOUNT_NAME": "Account"}
                    ),
                    hide_index=True, use_container_width=True,
                )
            else:
                st.success("No unengaged accounts with active pipeline.")
        else:
            st.success("All accounts are engaged!")


with tab_playbook:
    st.subheader("Competitive Displacement Playbook")
    present_competitors = filtered[
        ~filtered["Competitor"].isin(["Unknown", "", "Greenfield", "Snowflake (existing)", "Proprietary", "Other"])
    ]["Competitor"].unique()
    if len(present_competitors) == 0:
        st.info("No identified competitors yet \u2014 focus on intel gathering first.")
    for comp in sorted(present_competitors):
        if comp in BATTLE_CARDS:
            card = BATTLE_CARDS[comp]
            acct_list = filtered[filtered["Competitor"] == comp]["ACCOUNT_NAME"].tolist()
            with st.expander(f"{COMPETITOR_DOTS.get(comp, chr(9898))} **{comp}** ({len(acct_list)} accounts) \u2014 Lead with: {card['lead']}"):
                st.markdown("**Displacement Tactics:**")
                for tip in card["tips"]:
                    st.markdown(f"- {tip}")
                st.markdown(f"**Accounts:** {', '.join(acct_list[:10])}{'...' if len(acct_list) > 10 else ''}")


with tab_editor:
    st.subheader("Account Intelligence Editor")
    st.caption("Edit competitor, engagement status, spend, and notes below. Changes save automatically.")

    display = filtered[["AE", "SE", "ACCOUNT_NAME",
                         "Competitor", "Est. Spend", "Confirmed Spend",
                         "Engaged", "Notes"]].copy()

    display["Competitor"] = display["Competitor"].apply(lambda c: f"{COMPETITOR_DOTS.get(c, chr(9898))} {c}")

    def sync_edits():
        edited = st.session_state.get("main_editor", None)
        if edited is None:
            return
        edited_rows = edited.get("edited_rows", {})
        for row_idx_str, changes in edited_rows.items():
            row_idx = int(row_idx_str)
            if row_idx >= len(display):
                continue
            acct = display.iloc[row_idx]["ACCOUNT_NAME"]
            if acct not in st.session_state["editor_overrides"]:
                st.session_state["editor_overrides"][acct] = {
                    "Competitor": "Unknown",
                    "Engaged": False,
                    "Est. Spend": "",
                    "Confirmed Spend": False,
                    "Notes": "",
                }
            for col, val in changes.items():
                if col == "Competitor":
                    clean = val
                    for dot in COMPETITOR_DOTS.values():
                        clean = clean.replace(dot, "").strip()
                    st.session_state["editor_overrides"][acct]["Competitor"] = clean
                else:
                    st.session_state["editor_overrides"][acct][col] = val
            save_edit(acct, st.session_state["editor_overrides"][acct])

    st.data_editor(
        display,
        column_config={
            "AE": st.column_config.TextColumn("AE", disabled=True, width="small"),
            "SE": st.column_config.TextColumn("SE", disabled=True, width="small"),
            "ACCOUNT_NAME": st.column_config.TextColumn("Account", disabled=True, width="medium"),
            "Competitor": st.column_config.SelectboxColumn(
                "Competitor", width="medium",
                options=[f"{COMPETITOR_DOTS.get(c, chr(9898))} {c}" for c in ALL_COMPETITORS],
            ),
            "Est. Spend": st.column_config.TextColumn("Est. Spend", width="small"),
            "Confirmed Spend": st.column_config.CheckboxColumn("Confirmed", width="small"),
            "Engaged": st.column_config.CheckboxColumn("Engaged", width="small"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
        },
        hide_index=True,
        use_container_width=True,
        height=800,
        num_rows="fixed",
        key="main_editor",
        on_change=sync_edits,
    )
    st.caption(f"Showing {len(filtered)} accounts")
