"""Airflow DAG for consuming Kafka topics into bronze Parquet."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="ingest_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["qpu", "bronze"],
) as dag:
    consume_telemetry = BashOperator(
        task_id="consume_telemetry_once",
        bash_command=f"cd {PROJECT_ROOT} && python -c \"import sys; sys.path.insert(0, 'ingestion/consumers'); from telemetry_consumer import TelemetryConsumer; TelemetryConsumer().consume_once()\"",
    )
    consume_calibration = BashOperator(
        task_id="consume_calibration_once",
        bash_command=f"cd {PROJECT_ROOT} && python -c \"import sys; sys.path.insert(0, 'ingestion/consumers'); from calibration_consumer import CalibrationConsumer; CalibrationConsumer().consume_once()\"",
    )
    consume_jobs = BashOperator(
        task_id="consume_jobs_once",
        bash_command=f"cd {PROJECT_ROOT} && python -c \"import sys; sys.path.insert(0, 'ingestion/consumers'); from jobs_consumer import JobsConsumer; JobsConsumer().consume_once()\"",
    )
    consume_health = BashOperator(
        task_id="consume_health_once",
        bash_command=f"cd {PROJECT_ROOT} && python -c \"import sys; sys.path.insert(0, 'ingestion/consumers'); from health_consumer import HealthConsumer; HealthConsumer().consume_once()\"",
    )

    [consume_telemetry, consume_calibration, consume_jobs, consume_health]
