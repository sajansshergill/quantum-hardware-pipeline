{{ config(materialized='table') }}

with telemetry as (
    select *
    from {{ ref('stg_telemetry') }}
    where t2_within_physical_limit
      and two_qubit_error_expected
      and readout_fidelity_valid
),

with_baselines as (
    select
        t.*,
        d.baseline_T1_us,
        d.baseline_T2_us,
        d.baseline_gate_error_1q,
        d.baseline_gate_error_2q,
        d.baseline_readout_fidelity
    from telemetry t
    left join {{ ref('dim_device') }} d using (device_id)
)

select
    event_id,
    device_id,
    qubit_id,
    device_id || ':q' || cast(qubit_id as varchar) as qubit_key,
    T1_us,
    T2_us,
    gate_error_1q,
    gate_error_2q,
    readout_fidelity,
    timestamp,
    event_date,
    gate_error_1q / nullif(baseline_gate_error_1q, 0) as gate_error_1q_ratio,
    gate_error_2q / nullif(baseline_gate_error_2q, 0) as gate_error_2q_ratio,
    readout_fidelity - baseline_readout_fidelity as readout_fidelity_delta
from with_baselines
