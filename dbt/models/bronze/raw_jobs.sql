{{ config(materialized='view') }}

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
    cast(_ingested_at as varchar) as _ingested_at,
    cast(_source_topic as varchar) as _source_topic,
    cast(date as date) as event_date
from read_parquet(
    '{{ var("bronze_root") }}/jobs/*/*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
