{{ config(materialized='table') }}

select
    job_id,
    device_id,
    user_id,
    circuit_depth,
    num_shots,
    queue_wait_seconds,
    exec_time_seconds,
    total_time_seconds,
    status,
    error_code,
    sla_met,
    timestamp,
    event_date,
    abs(total_time_seconds - (queue_wait_seconds + exec_time_seconds)) <= 0.05 as total_time_valid,
    case
        when status in ('completed', 'cancelled') then error_code is null
        when status in ('failed', 'timeout') then error_code is not null
        else false
    end as error_code_valid
from {{ ref('raw_jobs') }}
where device_id is not null
  and queue_wait_seconds >= 0
  and exec_time_seconds >= 0
  and total_time_seconds >= 0
qualify row_number() over (
    partition by job_id
    order by timestamp desc
) = 1
