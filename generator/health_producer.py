"""
health_producer.py
------------------
Produces system health events to qpu.health.

Health events capture infrastructure-level signals for each QPU:
  - cryostat temperature stability
  - control electronics status
  - network connectivity
  - overall system health score

These are distinct from qubit-level telemetry — they reflect the
physical environment and control stack surrounding the QPU.

Schema
------
{
  "event_id":              str (UUID4),
  "device_id":             str,
  "cryo_temp_mk":          float,    # Cryostat temperature in millikelvin
  "cryo_temp_stable":      bool,
  "control_elec_status":   str ("nominal" | "degraded" | "fault"),
  "network_latency_ms":    float,
  "network_status":        str ("ok" | "degraded" | "down"),
  "system_health_score":   float,    # 0.0 (critical) – 1.0 (nominal)
  "active_alerts":         list[str],
  "timestamp":             str (ISO 8601)
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

# IBM Quantum Eagle/Heron devices run at ~15 mK
TARGET_CRYO_TEMP_MK = 15.0

CONTROL_ELEC_WEIGHTS  = {"nominal": 0.92, "degraded": 0.06, "fault": 0.02}
NETWORK_STATUS_WEIGHTS = {"ok": 0.95, "degraded": 0.04, "down": 0.01}

ALERT_POOL = [
    "HIGH_CRYO_TEMP",
    "CONTROL_ELEC_DEGRADED",
    "NETWORK_LATENCY_SPIKE",
    "QUBIT_COHERENCE_DROP",
    "READOUT_CHAIN_FAULT",
    "DILUTION_UNIT_WARNING",
]


def _weighted_choice(weight_map: dict) -> str:
    keys  = list(weight_map.keys())
    probs = list(weight_map.values())
    return random.choices(keys, weights=probs, k=1)[0]


def _compute_health_score(
    cryo_stable: bool,
    ctrl_status: str,
    net_status: str,
) -> float:
    score = 1.0
    if not cryo_stable:
        score -= 0.30
    if ctrl_status == "degraded":
        score -= 0.20
    elif ctrl_status == "fault":
        score -= 0.50
    if net_status == "degraded":
        score -= 0.10
    elif net_status == "down":
        score -= 0.40
    return round(max(0.0, score), 3)


def generate_health_batch() -> List[Tuple[dict, str]]:
    batch = []
    rng = np.random.default_rng()

    for device in DEVICE_REGISTRY:
        cryo_temp   = float(rng.normal(loc=TARGET_CRYO_TEMP_MK, scale=0.5))
        cryo_stable = abs(cryo_temp - TARGET_CRYO_TEMP_MK) < 1.0
        ctrl_status = _weighted_choice(CONTROL_ELEC_WEIGHTS)
        net_status  = _weighted_choice(NETWORK_STATUS_WEIGHTS)
        net_latency = float(rng.exponential(scale=5.0))  # avg 5ms

        health_score = _compute_health_score(cryo_stable, ctrl_status, net_status)

        # Surface alerts when health degrades
        active_alerts = []
        if not cryo_stable:
            active_alerts.append("HIGH_CRYO_TEMP")
        if ctrl_status != "nominal":
            active_alerts.append("CONTROL_ELEC_DEGRADED")
        if net_status != "ok":
            active_alerts.append("NETWORK_LATENCY_SPIKE")
        # Random rare alert
        if random.random() < 0.03:
            extra = random.choice(ALERT_POOL)
            if extra not in active_alerts:
                active_alerts.append(extra)

        record = {
            "event_id":            str(uuid.uuid4()),
            "device_id":           device.device_id,
            "cryo_temp_mk":        round(cryo_temp, 4),
            "cryo_temp_stable":    cryo_stable,
            "control_elec_status": ctrl_status,
            "network_latency_ms":  round(net_latency, 3),
            "network_status":      net_status,
            "system_health_score": health_score,
            "active_alerts":       active_alerts,
            "timestamp":           datetime.now(timezone.utc).isoformat(),
        }
        batch.append((record, device.device_id))

    logger.debug("Health batch: %d records", len(batch))
    return batch


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = BaseProducer("health")
    producer.run_loop(generate_health_batch, interval_seconds=15.0)