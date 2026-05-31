{{ config(materialized='table') }}

select
    device_id,
    qubit_id,
    device_id || ':q' || cast(qubit_id as varchar) as qubit_key
from {{ ref('dim_device') }}
cross join range(num_qubits) as qubits(qubit_id)
