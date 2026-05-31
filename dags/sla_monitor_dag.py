"""Airflow DAG for refreshing job SLA analytics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="sla_monitor_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["qpu", "sla"],
) as dag:
    refresh_gold_mart = BashOperator(
        task_id="refresh_device_reliability_mart",
        bash_command=f"cd {PROJECT_ROOT} && dbt run --project-dir dbt --profiles-dir dbt --select device_reliability_mart",
    )
