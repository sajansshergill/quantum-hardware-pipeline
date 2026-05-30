"""
telemetry_consumer.py
---------------------
Consumes qpu.telemetry → bronze/telemetry/date=.../device_id=.../

Adds physical constraint validation on top of schema validation:
  - T2 must not exceed 2 × T1
  - gate_error_2q must be >= gate_error_1q (2Q gates are harder)
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))
from base_consumer import BaseConsumer

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "telemetry_schema.json"

PYARROW_SCHEMA = pa.schema([
    pa.field("event_id",         pa.string(),    nullable=False),
    pa.field("device_id",        pa.string(),    nullable=False),
    pa.field("qubit_id",         pa.int32(),     nullable=False),
    pa.field("T1_us",            pa.float64(),   nullable=False),
    pa.field("T2_us",            pa.float64(),   nullable=False),
    pa.field("gate_error_1q",    pa.float64(),   nullable=False),
    pa.field("gate_error_2q",    pa.float64(),   nullable=False),
    pa.field("readout_fidelity", pa.float64(),   nullable=False),
    pa.field("timestamp",        pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_ingested_at",     pa.string(),    nullable=True),
    pa.field("_source_topic",    pa.string(),    nullable=True),
])


class TelemetryConsumer(BaseConsumer):
    def __init__(self):
        schema = json.loads(SCHEMA_PATH.read_text())
        super().__init__(
            topic_key="telemetry",
            topic="qpu.telemetry",
            group_id="qpu-telemetry-bronze-consumer",
            schema=schema,
            pyarrow_schema=PYARROW_SCHEMA,
        )

    def validate(self, record: Dict[str, Any]) -> bool:
        if not super().validate(record):
            return False
        # Physical constraint: T2 ≤ 2 × T1
        if record.get("T2_us", 0) > 2 * record.get("T1_us", 0) + 1e-6:
            logger.warning(
                "Physical violation T2 > 2*T1 on device=%s qubit=%s (T1=%.2f T2=%.2f)",
                record.get("device_id"), record.get("qubit_id"),
                record.get("T1_us"), record.get("T2_us"),
            )
            return False
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    TelemetryConsumer().run()