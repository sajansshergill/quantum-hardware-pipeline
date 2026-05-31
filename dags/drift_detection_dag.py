"""Airflow DAG for rolling drift detection."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="drift_detection_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["qpu", "drift"],
) as dag:
    detect_drift = BashOperator(
        task_id="detect_drift",
        bash_command=f"cd {PROJECT_ROOT} && python detection/drift_detector.py",
    )
