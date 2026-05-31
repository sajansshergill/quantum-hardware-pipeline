{{ config(materialized='table') }}

select
    event_id,
    device_id,
    qubit_id,
    T1_us,
    T2_us,
    gate_error_1q,
    gate_error_2q,
    readout_fidelity,
    timestamp,
    event_date,
    T2_us <= 2 * T1_us as t2_within_physical_limit,
    gate_error_2q >= gate_error_1q as two_qubit_error_expected,
    readout_fidelity between 0.5 and 1.0 as readout_fidelity_valid
from {{ ref('raw_telemetry') }}
where device_id is not null
  and qubit_id is not null
  and T1_us > 0
  and T2_us > 0
  and gate_error_1q >= 0
  and gate_error_2q >= 0
  and readout_fidelity between 0.5 and 1.0
qualify row_number() over (
    partition by event_id
    order by timestamp desc
) = 1
