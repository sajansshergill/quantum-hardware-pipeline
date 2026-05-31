"""Streamlit dashboard for the quantum hardware pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "data/lakehouse/qpu_pipeline.duckdb"))
ALERTS_PATH = Path(os.getenv("DRIFT_ALERTS_PATH", "data/alerts/drift_alerts.json"))


@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        return pd.DataFrame()
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        return conn.execute(sql).df()


def relation(table_name: str) -> str:
    if not DUCKDB_PATH.exists():
        return table_name
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        row = conn.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            where table_name = ?
            order by table_schema
            limit 1
            """,
            [table_name],
        ).fetchone()
    if not row:
        return table_name
    return f'"{row[0]}"."{row[1]}"'


def load_alerts() -> pd.DataFrame:
    if not ALERTS_PATH.exists():
        return pd.DataFrame()
    return pd.DataFrame(json.loads(ALERTS_PATH.read_text()))


st.set_page_config(page_title="QPU Reliability Dashboard", layout="wide")
st.title("Quantum Hardware Performance Intelligence")

if not DUCKDB_PATH.exists():
    st.info("No DuckDB database found yet. Run `make sample && make dbt` first.")
    st.stop()

mart = relation("device_reliability_mart")
telemetry = relation("fct_telemetry")
jobs = relation("stg_jobs")
health = relation("stg_health")

mart_df = query(f"select * from {mart} order by date desc, device_id")
if mart_df.empty:
    st.info("Gold mart is empty. Generate sample data and run dbt.")
    st.stop()

latest = mart_df.sort_values("date").groupby("device_id", as_index=False).tail(1)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Devices", latest["device_id"].nunique())
col2.metric("Avg T1 us", f"{latest['avg_T1_us'].mean():.1f}")
col3.metric("Min Readout Fidelity", f"{latest['min_readout_fidelity'].min():.4f}")
col4.metric("Job SLA", f"{latest['job_sla_pct'].mean():.1f}%")

st.subheader("Device Reliability Mart")
st.dataframe(mart_df, use_container_width=True)

tab1, tab2, tab3, tab4 = st.tabs(["Telemetry", "Jobs", "Health", "Drift Alerts"])

with tab1:
    trend = query(
        f"""
        select timestamp, device_id, avg(T1_us) as avg_T1_us, avg(T2_us) as avg_T2_us,
               avg(gate_error_1q) as avg_gate_error_1q,
               avg(gate_error_2q) as avg_gate_error_2q
        from {telemetry}
        group by 1, 2
        order by 1
        """
    )
    st.line_chart(trend, x="timestamp", y=["avg_T1_us", "avg_T2_us"], color="device_id")
    st.line_chart(trend, x="timestamp", y=["avg_gate_error_1q", "avg_gate_error_2q"], color="device_id")

with tab2:
    jobs_df = query(
        f"""
        select device_id, status, count(*) as jobs, round(100.0 * avg(cast(sla_met as double)), 2) as sla_pct
        from {jobs}
        group by 1, 2
        order by 1, 2
        """
    )
    st.dataframe(jobs_df, use_container_width=True)

with tab3:
    health_df = query(
        f"""
        select timestamp, device_id, cryo_temp_mk, network_latency_ms,
               system_health_score, control_elec_status, network_status
        from {health}
        order by timestamp desc
        limit 500
        """
    )
    st.dataframe(health_df, use_container_width=True)

with tab4:
    alerts = load_alerts()
    if alerts.empty:
        st.info("No drift alerts written yet. Run `python detection/drift_detector.py` after dbt.")
    else:
        st.dataframe(alerts, use_container_width=True)
