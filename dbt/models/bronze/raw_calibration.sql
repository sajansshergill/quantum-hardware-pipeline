{{ config(materialized='view') }}

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
    cast(_ingested_at as varchar) as _ingested_at,
    cast(_source_topic as varchar) as _source_topic,
    cast(date as date) as event_date
from read_parquet(
    '{{ var("bronze_root") }}/calibration/*/*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
