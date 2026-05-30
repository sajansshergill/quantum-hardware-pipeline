"""
qpu_devices.py
--------------
Device registry for the synthetic QPU fleet.
Each device has baseline performance characteristics that the generators
use to produce realistic (but synthetic) telemetry with per-device variance.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class QPUDevice:
    device_id: str
    num_qubits: int
    backend_version: str
    region: str
    # Baseline performance (generators add Gaussian noise around these)
    baseline_T1_us: float          # Relaxation time (microseconds)
    baseline_T2_us: float          # Dephasing time (microseconds)
    baseline_gate_error_1q: float  # Single-qubit gate error rate
    baseline_gate_error_2q: float  # Two-qubit gate error rate
    baseline_readout_fidelity: float
    # Noise scale (std dev as fraction of baseline)
    noise_scale: float = 0.05


DEVICE_REGISTRY: List[QPUDevice] = [
    QPUDevice(
        device_id="ibm_fez",
        num_qubits=127,
        backend_version="1.3.2",
        region="us-east",
        baseline_T1_us=180.0,
        baseline_T2_us=120.0,
        baseline_gate_error_1q=0.0008,
        baseline_gate_error_2q=0.006,
        baseline_readout_fidelity=0.985,
    ),
    QPUDevice(
        device_id="ibm_torino",
        num_qubits=133,
        backend_version="2.0.1",
        region="eu-west",
        baseline_T1_us=210.0,
        baseline_T2_us=150.0,
        baseline_gate_error_1q=0.0006,
        baseline_gate_error_2q=0.005,
        baseline_readout_fidelity=0.990,
    ),
    QPUDevice(
        device_id="ibm_kyiv",
        num_qubits=127,
        backend_version="1.2.8",
        region="us-south",
        baseline_T1_us=160.0,
        baseline_T2_us=100.0,
        baseline_gate_error_1q=0.0010,
        baseline_gate_error_2q=0.008,
        baseline_readout_fidelity=0.978,
        noise_scale=0.07,   # slightly noisier device
    ),
    QPUDevice(
        device_id="ibm_osaka",
        num_qubits=127,
        backend_version="1.4.0",
        region="ap-northeast",
        baseline_T1_us=195.0,
        baseline_T2_us=130.0,
        baseline_gate_error_1q=0.0007,
        baseline_gate_error_2q=0.0055,
        baseline_readout_fidelity=0.987,
    ),
]

DEVICE_MAP = {d.device_id: d for d in DEVICE_REGISTRY}


def get_device(device_id: str) -> QPUDevice:
    if device_id not in DEVICE_MAP:
        raise ValueError(f"Unknown device: {device_id}. Available: {list(DEVICE_MAP)}")
    return DEVICE_MAP[device_id]