{{ config(materialized='view') }}

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
    cast(_ingested_at as varchar) as _ingested_at,
    cast(_source_topic as varchar) as _source_topic,
    cast(date as date) as event_date
from read_parquet(
    '{{ var("bronze_root") }}/health/*/*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
