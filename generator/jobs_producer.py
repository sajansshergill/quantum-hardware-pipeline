"""
jobs_producer.py
----------------
Produces quantum job execution records to qpu.jobs.

Each record represents one job submitted by a user or application to a QPU:
  - which device ran the job
  - circuit depth and shot count
  - queue wait time and execution time
  - final status and error info if failed
  - SLA compliance flag (execution completed within the promised window)

Schema
------
{
  "job_id":              str (UUID4),
  "device_id":           str,
  "user_id":             str (anonymized),
  "circuit_depth":       int,
  "num_shots":           int,
  "queue_wait_seconds":  float,
  "exec_time_seconds":   float,
  "total_time_seconds":  float,
  "status":              str ("completed" | "failed" | "cancelled" | "timeout"),
  "error_code":          str | null,
  "sla_met":             bool,
  "timestamp":           str (ISO 8601)
}

SLA definition: total_time_seconds ≤ 300s (5 min) for standard jobs.
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

SLA_THRESHOLD_SECONDS = 300.0

STATUS_WEIGHTS = {
    "completed": 0.88,
    "failed":    0.07,
    "cancelled": 0.03,
    "timeout":   0.02,
}

ERROR_CODES = {
    "failed":  ["ERR_GATE_CALIBRATION", "ERR_READOUT_NOISE", "ERR_TRANSPILE_FAIL", "ERR_BACKEND_BUSY"],
    "timeout": ["ERR_JOB_TIMEOUT"],
}


def _weighted_choice(weight_map: dict) -> str:
    keys  = list(weight_map.keys())
    probs = list(weight_map.values())
    return random.choices(keys, weights=probs, k=1)[0]


def _anonymized_user() -> str:
    return "user_" + uuid.uuid4().hex[:8]


def generate_jobs_batch(jobs_per_device: int = 5) -> List[Tuple[dict, str]]:
    """
    Simulate a burst of jobs arriving per device.
    In a real system this would be driven by the actual job queue.
    """
    batch = []
    rng = np.random.default_rng()

    for device in DEVICE_REGISTRY:
        for _ in range(jobs_per_device):
            status     = _weighted_choice(STATUS_WEIGHTS)
            circuit_d  = int(rng.integers(5, 500))
            num_shots  = int(rng.choice([512, 1024, 2048, 4096, 8192]))
            queue_wait = float(rng.exponential(scale=30.0))       # avg 30s queue
            exec_time  = float(rng.gamma(shape=2.0, scale=20.0))  # avg 40s exec
            total_time = queue_wait + exec_time

            error_code = None
            if status in ERROR_CODES:
                error_code = random.choice(ERROR_CODES[status])

            record = {
                "job_id":             str(uuid.uuid4()),
                "device_id":          device.device_id,
                "user_id":            _anonymized_user(),
                "circuit_depth":      circuit_d,
                "num_shots":          num_shots,
                "queue_wait_seconds": round(queue_wait, 2),
                "exec_time_seconds":  round(exec_time, 2),
                "total_time_seconds": round(total_time, 2),
                "status":             status,
                "error_code":         error_code,
                "sla_met":            total_time <= SLA_THRESHOLD_SECONDS,
                "timestamp":          datetime.now(timezone.utc).isoformat(),
            }
            batch.append((record, device.device_id))

    logger.debug("Jobs batch: %d records", len(batch))
    return batch


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = BaseProducer("jobs")
    producer.run_loop(generate_jobs_batch, interval_seconds=5.0)