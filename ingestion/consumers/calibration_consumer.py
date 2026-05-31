"""
calibration_consumer.py
-----------------------
Consumes qpu.calibration → bronze/calibration/date=.../device_id=.../

Extra validation:
  - qubits_calibrated <= qubits_total
  - duration_seconds in plausible range (60s – 3600s)
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))
from base_consumer import BaseConsumer

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "calibration_schema.json"

PYARROW_SCHEMA = pa.schema([
    pa.field("run_id",             pa.string(),  nullable=False),
    pa.field("device_id",          pa.string(),  nullable=False),
    pa.field("triggered_by",       pa.string(),  nullable=False),
    pa.field("duration_seconds",   pa.float64(), nullable=False),
    pa.field("qubits_total",       pa.int32(),   nullable=False),
    pa.field("qubits_calibrated",  pa.int32(),   nullable=False),
    pa.field("pre_cal_drift_flag", pa.bool_(),   nullable=False),
    pa.field("status",             pa.string(),  nullable=False),
    pa.field("backend_version",    pa.string(),  nullable=False),
    pa.field("timestamp",          pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_ingested_at",       pa.string(),  nullable=True),
    pa.field("_source_topic",      pa.string(),  nullable=True),
])


class CalibrationConsumer(BaseConsumer):
    def __init__(self, bronze_root=None, bootstrap_servers=None):
        schema = json.loads(SCHEMA_PATH.read_text())
        super().__init__(
            topic_key="calibration",
            topic="qpu.calibration",
            group_id="qpu-calibration-bronze-consumer",
            schema=schema,
            pyarrow_schema=PYARROW_SCHEMA,
            bronze_root=bronze_root,
            bootstrap_servers=bootstrap_servers,
        )

    def validate(self, record: Dict[str, Any]) -> bool:
        if not super().validate(record):
            return False
        if record.get("qubits_calibrated", 0) > record.get("qubits_total", 0):
            logger.warning("qubits_calibrated > qubits_total on run_id=%s", record.get("run_id"))
            return False
        dur = record.get("duration_seconds", 0)
        if not (60 <= dur <= 3600):
            logger.warning("Implausible calibration duration=%.1fs on run_id=%s", dur, record.get("run_id"))
            return False
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    CalibrationConsumer().run()