# Majors Acquisition — Revenue Projections

Streamlit app deployed at `SNOWPUBLIC.STREAMLIT.MAJORS_ACQ_PIPELINE` in Snowhouse.

## Deploy

Always deploy using the Snowhouse connection. The `query_warehouse` is set to `STREAMLIT`
in `snowflake.yml` because the app runs as `EXECUTE AS OWNER` and the owner role must have
direct USAGE on the warehouse (no secondary roles).

```bash
cd /path/to/repo
snow streamlit deploy majors_acq_pipeline --connection Snowhouse --replace
```

> **Note**: Do NOT change `query_warehouse` to `STREAMLIT_XSMALL_SIS` in `snowflake.yml`.
> The app owner does not have direct USAGE on that warehouse, which will break the app.

## Backing Tables (all in `SNOWPUBLIC.STREAMLIT`)

| Table | Owner | Purpose |
|-------|-------|---------|
| `PIPELINE` | `DASHBOARD_SHARING_RL` | Open opportunities (247 rows, 26 AEs) |
| `TEAM_ROSTER` | `DASHBOARD_SHARING_RL` | AE/SE/DM assignments by district |
| `CLOSE_RATES` | `DASHBOARD_SHARING_RL` | Historical win rates by stage |
| `LEADERSHIP` | `DASHBOARD_SHARING_RL` | RVP-level leadership (2 rows) |

## Updating Data

Use `DASHBOARD_SHARING_RL` to update any backing table. Example — updating a DM name:

```sql
USE ROLE DASHBOARD_SHARING_RL;
UPDATE SNOWPUBLIC.STREAMLIT.TEAM_ROSTER
SET DM_NAME = '<new_dm>'
WHERE DM_NAME = '<old_dm>'
  AND DISTRICT = '<district>';
```