select *
from {{ ref('fct_telemetry') }}
where readout_fidelity < 0.5
   or readout_fidelity > 1.0
