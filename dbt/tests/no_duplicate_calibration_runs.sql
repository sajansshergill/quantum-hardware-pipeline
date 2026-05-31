select run_id, count(*) as row_count
from {{ ref('stg_calibration') }}
group by run_id
having count(*) > 1
