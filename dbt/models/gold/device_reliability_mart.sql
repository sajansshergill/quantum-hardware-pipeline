{{ config(materialized='table') }}

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
    from {{ ref('fct_telemetry') }}
    group by 1, 2
),

calibration_daily as (
    select
        device_id,
        event_date as date,
        count(*) as calibration_count,
        sum(case when status = 'success' then 1 else 0 end) as successful_calibration_count,
        max(cast(pre_cal_drift_flag as integer)) = 1 as pre_cal_drift_seen
    from {{ ref('stg_calibration') }}
    group by 1, 2
),

jobs_daily as (
    select
        device_id,
        event_date as date,
        count(*) as job_count,
        100.0 * avg(cast(sla_met as double)) as job_sla_pct
    from {{ ref('stg_jobs') }}
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
    from {{ ref('stg_health') }}
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
