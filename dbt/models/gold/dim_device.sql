{{ config(materialized='table') }}

select *
from (
    values
        ('ibm_fez', 127, '1.3.2', 'us-east', 180.0, 120.0, 0.0008, 0.0060, 0.985),
        ('ibm_torino', 133, '2.0.1', 'eu-west', 210.0, 150.0, 0.0006, 0.0050, 0.990),
        ('ibm_kyiv', 127, '1.2.8', 'us-south', 160.0, 100.0, 0.0010, 0.0080, 0.978),
        ('ibm_osaka', 127, '1.4.0', 'ap-northeast', 195.0, 130.0, 0.0007, 0.0055, 0.987)
) as t(
    device_id,
    num_qubits,
    backend_version,
    region,
    baseline_T1_us,
    baseline_T2_us,
    baseline_gate_error_1q,
    baseline_gate_error_2q,
    baseline_readout_fidelity
)
