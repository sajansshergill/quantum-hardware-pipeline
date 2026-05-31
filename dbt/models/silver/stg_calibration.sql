{{ config(materialized='table') }}

select
    run_id,
    device_id,
    triggered_by,
    duration_seconds,
    qubits_total,
    qubits_calibrated,
    pre_cal_drift_flag,
    status,
    backend_version,
    timestamp,
    event_date,
    qubits_calibrated <= qubits_total as qubit_count_valid,
    duration_seconds between 60 and 3600 as duration_valid
from {{ ref('raw_calibration') }}
where device_id is not null
  and qubits_total > 0
  and qubits_calibrated between 0 and qubits_total
  and duration_seconds between 60 and 3600
qualify row_number() over (
    partition by run_id
    order by timestamp desc
) = 1
