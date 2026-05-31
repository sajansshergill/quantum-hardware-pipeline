"""
health_consumer.py
------------------
Consumes qpu.health → bronze/health/date=.../device_id=.../

Extra validation:
  - system_health_score in [0.0, 1.0]
  - cryo_temp_mk plausible for dilution refrigerator (5–100 mK)
  - active_alerts is a list
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

SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "health_schema.json"

PYARROW_SCHEMA = pa.schema([
    pa.field("event_id",            pa.string(),  nullable=False),
    pa.field("device_id",           pa.string(),  nullable=False),
    pa.field("cryo_temp_mk",        pa.float64(), nullable=False),
    pa.field("cryo_temp_stable",    pa.bool_(),   nullable=False),
    pa.field("control_elec_status", pa.string(),  nullable=False),
    pa.field("network_latency_ms",  pa.float64(), nullable=False),
    pa.field("network_status",      pa.string(),  nullable=False),
    pa.field("system_health_score", pa.float64(), nullable=False),
    pa.field("active_alerts",       pa.list_(pa.string()), nullable=False),
    pa.field("timestamp",           pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_ingested_at",        pa.string(),  nullable=True),
    pa.field("_source_topic",       pa.string(),  nullable=True),
])


class HealthConsumer(BaseConsumer):
    def __init__(self, bronze_root=None, bootstrap_servers=None):
        schema = json.loads(SCHEMA_PATH.read_text())
        super().__init__(
            topic_key="health",
            topic="qpu.health",
            group_id="qpu-health-bronze-consumer",
            schema=schema,
            pyarrow_schema=PYARROW_SCHEMA,
            bronze_root=bronze_root,
            bootstrap_servers=bootstrap_servers,
        )

    def validate(self, record: Dict[str, Any]) -> bool:
        if not super().validate(record):
            return False

        score = record.get("system_health_score", -1)
        if not (0.0 <= score <= 1.0):
            logger.warning("health_score out of range %.3f on event_id=%s", score, record.get("event_id"))
            return False

        cryo = record.get("cryo_temp_mk", -1)
        if not (5.0 <= cryo <= 100.0):
            logger.warning("Implausible cryo_temp_mk=%.4f on event_id=%s", cryo, record.get("event_id"))
            return False

        if not isinstance(record.get("active_alerts"), list):
            logger.warning("active_alerts is not a list on event_id=%s", record.get("event_id"))
            return False

        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    HealthConsumer().run()