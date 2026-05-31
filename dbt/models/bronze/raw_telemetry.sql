{{ config(materialized='view') }}

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
    cast(_ingested_at as varchar) as _ingested_at,
    cast(_source_topic as varchar) as _source_topic,
    cast(date as date) as event_date
from read_parquet(
    '{{ var("bronze_root") }}/telemetry/*/*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
