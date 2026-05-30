"""
telemetry_producer.py
---------------------
Produces per-qubit hardware telemetry to qpu.telemetry.

Each event represents one telemetry snapshot for one qubit on one device:
  - T1 (relaxation time, microseconds)
  - T2 (dephasing time, microseconds)
  - 1Q gate error rate
  - 2Q gate error rate
  - Readout fidelity

Partitioned by device_id so all qubits from a device land in the same partition.

Schema
------
{
  "event_id":           str (UUID4),
  "device_id":          str,
  "qubit_id":           int,
  "T1_us":              float,
  "T2_us":              float,
  "gate_error_1q":      float,
  "gate_error_2q":      float,
  "readout_fidelity":   float,
  "timestamp":          str (ISO 8601)
}
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Tuple

import numpy as np

from base_producer import BaseProducer
from qpu_devices import DEVICE_REGISTRY, QPUDevice

logger = logging.getLogger(__name__)


def _sample_qubit_metrics(device: QPUDevice, qubit_id: int) -> dict:
    """
    Draw a single telemetry reading for one qubit.
    Uses Gaussian noise scaled by device.noise_scale.
    Clamps to physically plausible bounds.
    """
    rng = np.random.default_rng()
    ns = device.noise_scale

    def noisy(baseline: float, low: float, high: float) -> float:
        val = rng.normal(loc=baseline, scale=baseline * ns)
        return float(np.clip(val, low, high))

    # T2 is always ≤ 2*T1 (physical constraint)
    T1 = noisy(device.baseline_T1_us, 10.0, 500.0)
    T2 = noisy(device.baseline_T2_us, 5.0, 2 * T1)

    return {
        "event_id":         str(uuid.uuid4()),
        "device_id":        device.device_id,
        "qubit_id":         qubit_id,
        "T1_us":            round(T1, 3),
        "T2_us":            round(T2, 3),
        "gate_error_1q":    round(noisy(device.baseline_gate_error_1q, 1e-5, 0.05), 6),
        "gate_error_2q":    round(noisy(device.baseline_gate_error_2q, 1e-4, 0.15), 6),
        "readout_fidelity": round(noisy(device.baseline_readout_fidelity, 0.5, 1.0), 5),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


def generate_telemetry_batch() -> List[Tuple[dict, str]]:
    """
    For every device, emit one telemetry record per qubit.
    Returns list of (record, partition_key) tuples.
    """
    batch = []
    for device in DEVICE_REGISTRY:
        for qubit_id in range(device.num_qubits):
            record = _sample_qubit_metrics(device, qubit_id)
            batch.append((record, device.device_id))
    logger.debug("Telemetry batch: %d records across %d devices", len(batch), len(DEVICE_REGISTRY))
    return batch


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = BaseProducer("telemetry")
    producer.run_loop(generate_telemetry_batch, interval_seconds=10.0)