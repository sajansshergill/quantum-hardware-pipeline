"""Airflow DAG for dbt transformations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="transform_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["qpu", "dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_ROOT} && dbt run --project-dir dbt --profiles-dir dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_ROOT} && dbt test --project-dir dbt --profiles-dir dbt",
    )

    dbt_run >> dbt_test
