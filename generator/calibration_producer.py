"""
calibration_producer.py
-----------------------
Produces calibration run records to qpu.calibration.

A calibration run happens on a schedule (or on demand) for a full device.
Each event captures the outcome of one calibration cycle:
  - which device was calibrated
  - duration of the run
  - number of qubits successfully recalibrated
  - whether drift was detected pre-calibration
  - run status (success / partial / failed)

Real IBM Quantum devices calibrate every few hours; we emit one record
per device per loop iteration to simulate that cadence.

Schema
------
{
  "run_id":             str (UUID4),
  "device_id":          str,
  "triggered_by":       str ("scheduled" | "manual" | "drift_alert"),
  "duration_seconds":   float,
  "qubits_total":       int,
  "qubits_calibrated":  int,
  "pre_cal_drift_flag": bool,
  "status":             str ("success" | "partial" | "failed"),
  "backend_version":    str,
  "timestamp":          str (ISO 8601)
}
"""

import logging
import random
import uuid
from datetime import datetime, timezone
from typing import List, Tuple

import numpy as np

from base_producer import BaseProducer
from qpu_devices import DEVICE_REGISTRY

logger = logging.getLogger(__name__)

TRIGGER_WEIGHTS = {"scheduled": 0.80, "manual": 0.12, "drift_alert": 0.08}
STATUS_WEIGHTS  = {"success": 0.88, "partial": 0.10, "failed": 0.02}


def _weighted_choice(weight_map: dict) -> str:
    keys   = list(weight_map.keys())
    probs  = list(weight_map.values())
    return random.choices(keys, weights=probs, k=1)[0]


def generate_calibration_batch() -> List[Tuple[dict, str]]:
    batch = []
    rng = np.random.default_rng()

    for device in DEVICE_REGISTRY:
        status        = _weighted_choice(STATUS_WEIGHTS)
        qubits_total  = device.num_qubits
        drift_flag    = random.random() < 0.15   # ~15% of runs catch pre-existing drift

        # Partial calibrations recalibrate fewer qubits
        if status == "success":
            qubits_cal = qubits_total
        elif status == "partial":
            qubits_cal = int(rng.integers(qubits_total // 2, qubits_total))
        else:
            qubits_cal = int(rng.integers(0, qubits_total // 4))

        record = {
            "run_id":             str(uuid.uuid4()),
            "device_id":          device.device_id,
            "triggered_by":       _weighted_choice(TRIGGER_WEIGHTS),
            "duration_seconds":   round(float(rng.uniform(120.0, 900.0)), 1),
            "qubits_total":       qubits_total,
            "qubits_calibrated":  qubits_cal,
            "pre_cal_drift_flag": drift_flag,
            "status":             status,
            "backend_version":    device.backend_version,
            "timestamp":          datetime.now(timezone.utc).isoformat(),
        }
        batch.append((record, device.device_id))

    logger.debug("Calibration batch: %d records", len(batch))
    return batch


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = BaseProducer("calibration")
    # Calibration runs are less frequent than telemetry
    producer.run_loop(generate_calibration_batch, interval_seconds=60.0)