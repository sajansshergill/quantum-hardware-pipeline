select *
from {{ ref('device_reliability_mart') }}
where device_id is null
