"""Build the local DuckDB demo dataset used by Streamlit Cloud."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


class DemoDataBuildError(RuntimeError):
    """Raised when the demo-data build fails."""


def build_demo_data(run_dbt_tests: bool = False, use_dbt: bool = False) -> None:
    """Generate sample bronze data, build DuckDB models, and write drift alerts."""
    sys.path.insert(0, str(ROOT))

    from detection.drift_detector import run_drift_detection
    from scripts.generate_bronze_sample import main as generate_bronze_sample

    generate_bronze_sample()
    if use_dbt:
        _run_command(["dbt", "run", "--project-dir", "dbt", "--profiles-dir", "dbt"])
        if run_dbt_tests:
            _run_command(["dbt", "test", "--project-dir", "dbt", "--profiles-dir", "dbt"])
    else:
        build_duckdb_models()
    run_drift_detection(duckdb_path=_duckdb_path())


def build_duckdb_models() -> None:
    """Build the same demo mart as dbt, without shelling out to dbt."""
    db_path = _duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_root = _project_path(os.getenv("BRONZE_ROOT", "data/bronze"))

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("create schema if not exists bronze")
        conn.execute("create schema if not exists silver")
        conn.execute("create schema if not exists gold")
        conn.execute(
            f"""
            create or replace view bronze.raw_telemetry as
            select
                event_id,
                device_id,
                cast(qubit_id as integer) as qubit_id,
                cast(T1_us as double) as T1_us,
                cast(T2_us as double) as T2_us,
                cast(gate_error_1q as double) as gate_error_1q,
                cast(gate_error_2q as double) as gate_error_2q,
                cast(readout_fidelity as double) as readout_fidelity,
                cast(timestamp as timestamptz) as timestamp,
                cast(date as date) as event_date
            from read_parquet('{bronze_root}/telemetry/*/*/*.parquet', hive_partitioning = true, union_by_name = true)
            """
        )
        conn.execute(
            f"""
            create or replace view bronze.raw_calibration as
            select
                run_id,
                device_id,
                cast(triggered_by as varchar) as triggered_by,
                cast(duration_seconds as double) as duration_seconds,
                cast(qubits_total as integer) as qubits_total,
                cast(qubits_calibrated as integer) as qubits_calibrated,
                cast(pre_cal_drift_flag as boolean) as pre_cal_drift_flag,
                cast(status as varchar) as status,
                cast(backend_version as varchar) as backend_version,
                cast(timestamp as timestamptz) as timestamp,
                cast(date as date) as event_date
            from read_parquet('{bronze_root}/calibration/*/*/*.parquet', hive_partitioning = true, union_by_name = true)
            """
        )
        conn.execute(
            f"""
            create or replace view bronze.raw_jobs as
            select
                job_id,
                device_id,
                cast(user_id as varchar) as user_id,
                cast(circuit_depth as integer) as circuit_depth,
                cast(num_shots as integer) as num_shots,
                cast(queue_wait_seconds as double) as queue_wait_seconds,
                cast(exec_time_seconds as double) as exec_time_seconds,
                cast(total_time_seconds as double) as total_time_seconds,
                cast(status as varchar) as status,
                cast(error_code as varchar) as error_code,
                cast(sla_met as boolean) as sla_met,
                cast(timestamp as timestamptz) as timestamp,
                cast(date as date) as event_date
            from read_parquet('{bronze_root}/jobs/*/*/*.parquet', hive_partitioning = true, union_by_name = true)
            """
        )
        conn.execute(
            f"""
            create or replace view bronze.raw_health as
            select
                event_id,
                device_id,
                cast(cryo_temp_mk as double) as cryo_temp_mk,
                cast(cryo_temp_stable as boolean) as cryo_temp_stable,
                cast(control_elec_status as varchar) as control_elec_status,
                cast(network_latency_ms as double) as network_latency_ms,
                cast(network_status as varchar) as network_status,
                cast(system_health_score as double) as system_health_score,
                active_alerts,
                cast(timestamp as timestamptz) as timestamp,
                cast(date as date) as event_date
            from read_parquet('{bronze_root}/health/*/*/*.parquet', hive_partitioning = true, union_by_name = true)
            """
        )
        conn.execute(
            """
            create or replace table gold.dim_device as
            select *
            from (
                values
                    ('ibm_fez', 127, '1.3.2', 'us-east', 180.0, 120.0, 0.0008, 0.0060, 0.985),
                    ('ibm_torino', 133, '2.0.1', 'eu-west', 210.0, 150.0, 0.0006, 0.0050, 0.990),
                    ('ibm_kyiv', 127, '1.2.8', 'us-south', 160.0, 100.0, 0.0010, 0.0080, 0.978),
                    ('ibm_osaka', 127, '1.4.0', 'ap-northeast', 195.0, 130.0, 0.0007, 0.0055, 0.987)
            ) as t(device_id, num_qubits, backend_version, region, baseline_T1_us, baseline_T2_us,
                   baseline_gate_error_1q, baseline_gate_error_2q, baseline_readout_fidelity)
            """
        )
        conn.execute(
            """
            create or replace table silver.stg_telemetry as
            select
                *,
                T2_us <= 2 * T1_us as t2_within_physical_limit,
                gate_error_2q >= gate_error_1q as two_qubit_error_expected,
                readout_fidelity between 0.5 and 1.0 as readout_fidelity_valid
            from bronze.raw_telemetry
            qualify row_number() over (partition by event_id order by timestamp desc) = 1
            """
        )
        conn.execute(
            """
            create or replace table silver.stg_calibration as
            select
                *,
                qubits_calibrated <= qubits_total as qubit_count_valid,
                duration_seconds between 60 and 3600 as duration_valid
            from bronze.raw_calibration
            qualify row_number() over (partition by run_id order by timestamp desc) = 1
            """
        )
        conn.execute(
            """
            create or replace table silver.stg_jobs as
            select
                *,
                abs(total_time_seconds - (queue_wait_seconds + exec_time_seconds)) <= 0.05 as total_time_valid,
                case
                    when status in ('completed', 'cancelled') then error_code is null
                    when status in ('failed', 'timeout') then error_code is not null
                    else false
                end as error_code_valid
            from bronze.raw_jobs
            qualify row_number() over (partition by job_id order by timestamp desc) = 1
            """
        )
        conn.execute(
            """
            create or replace table silver.stg_health as
            select
                *,
                system_health_score between 0 and 1 as health_score_valid,
                cryo_temp_mk between 5 and 100 as cryo_temp_valid
            from bronze.raw_health
            qualify row_number() over (partition by event_id order by timestamp desc) = 1
            """
        )
        conn.execute(
            """
            create or replace table gold.dim_qubit as
            select device_id, qubit_id, device_id || ':q' || cast(qubit_id as varchar) as qubit_key
            from gold.dim_device
            cross join range(num_qubits) as qubits(qubit_id)
            """
        )
        conn.execute(
            """
            create or replace table gold.fct_telemetry as
            select
                t.event_id,
                t.device_id,
                t.qubit_id,
                t.device_id || ':q' || cast(t.qubit_id as varchar) as qubit_key,
                t.T1_us,
                t.T2_us,
                t.gate_error_1q,
                t.gate_error_2q,
                t.readout_fidelity,
                t.timestamp,
                t.event_date,
                t.gate_error_1q / nullif(d.baseline_gate_error_1q, 0) as gate_error_1q_ratio,
                t.gate_error_2q / nullif(d.baseline_gate_error_2q, 0) as gate_error_2q_ratio,
                t.readout_fidelity - d.baseline_readout_fidelity as readout_fidelity_delta
            from silver.stg_telemetry t
            left join gold.dim_device d using (device_id)
            where t.t2_within_physical_limit
              and t.two_qubit_error_expected
              and t.readout_fidelity_valid
            """
        )
        conn.execute(
            """
            create or replace table gold.device_reliability_mart as
            with telemetry_daily as (
                select
                    device_id,
                    event_date as date,
                    avg(T1_us) as avg_T1_us,
                    avg(T2_us) as avg_T2_us,
                    quantile_cont(gate_error_1q, 0.99) as p99_gate_error_1q,
                    quantile_cont(gate_error_2q, 0.99) as p99_gate_error_2q,
                    min(readout_fidelity) as min_readout_fidelity,
                    count(*) as telemetry_count,
                    max(case when gate_error_1q_ratio >= 1.5 or gate_error_2q_ratio >= 1.5 then 1 else 0 end) = 1 as drift_flag
                from gold.fct_telemetry
                group by 1, 2
            ),
            calibration_daily as (
                select
                    device_id,
                    event_date as date,
                    count(*) as calibration_count,
                    sum(case when status = 'success' then 1 else 0 end) as successful_calibration_count,
                    max(cast(pre_cal_drift_flag as integer)) = 1 as pre_cal_drift_seen
                from silver.stg_calibration
                group by 1, 2
            ),
            jobs_daily as (
                select
                    device_id,
                    event_date as date,
                    count(*) as job_count,
                    100.0 * avg(cast(sla_met as double)) as job_sla_pct
                from silver.stg_jobs
                where total_time_valid and error_code_valid
                group by 1, 2
            ),
            health_daily as (
                select
                    device_id,
                    event_date as date,
                    avg(system_health_score) as avg_system_health_score,
                    min(system_health_score) as min_system_health_score,
                    max(case when control_elec_status != 'nominal' or network_status != 'ok' then 1 else 0 end) = 1 as health_alert_seen
                from silver.stg_health
                group by 1, 2
            )
            select
                t.device_id,
                t.date,
                round(t.avg_T1_us, 3) as avg_T1_us,
                round(t.avg_T2_us, 3) as avg_T2_us,
                round(t.p99_gate_error_1q, 6) as p99_gate_error_1q,
                round(t.p99_gate_error_2q, 6) as p99_gate_error_2q,
                round(t.min_readout_fidelity, 5) as min_readout_fidelity,
                t.telemetry_count,
                coalesce(c.calibration_count, 0) as calibration_count,
                coalesce(c.successful_calibration_count, 0) as successful_calibration_count,
                coalesce(c.pre_cal_drift_seen, false) or t.drift_flag as drift_flag,
                coalesce(j.job_count, 0) as job_count,
                round(coalesce(j.job_sla_pct, 100.0), 2) as job_sla_pct,
                round(coalesce(h.avg_system_health_score, 1.0), 3) as avg_system_health_score,
                round(coalesce(h.min_system_health_score, 1.0), 3) as min_system_health_score,
                coalesce(h.health_alert_seen, false) as health_alert_seen
            from telemetry_daily t
            left join calibration_daily c using (device_id, date)
            left join jobs_daily j using (device_id, date)
            left join health_daily h using (device_id, date)
            """
        )


def _run_command(command: list[str]) -> None:
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        output = "\n".join(part for part in (result.stdout, result.stderr) if part).strip()
        raise DemoDataBuildError(output[-4000:] or f"{command} failed with exit code {result.returncode}")


def _duckdb_path() -> Path:
    return _project_path(os.getenv("DUCKDB_PATH", "data/lakehouse/qpu_pipeline.duckdb"))


def _project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def main() -> None:
    build_demo_data(run_dbt_tests=True, use_dbt=True)


if __name__ == "__main__":
    main()
