{{ config(materialized='table') }}

select
    event_id,
    device_id,
    cryo_temp_mk,
    cryo_temp_stable,
    control_elec_status,
    network_latency_ms,
    network_status,
    system_health_score,
    active_alerts,
    timestamp,
    event_date,
    system_health_score between 0 and 1 as health_score_valid,
    cryo_temp_mk between 5 and 100 as cryo_temp_valid
from {{ ref('raw_health') }}
where device_id is not null
  and system_health_score between 0 and 1
  and cryo_temp_mk between 5 and 100
qualify row_number() over (
    partition by event_id
    order by timestamp desc
) = 1
