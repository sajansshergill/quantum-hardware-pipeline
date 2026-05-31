"""
jobs_consumer.py
----------------
Consumes qpu.jobs → bronze/jobs/date=.../device_id=.../

Extra validation:
  - total_time == queue_wait + exec_time (within tolerance)
  - error_code must be null for completed/cancelled jobs
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

SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "jobs_schema.json"

PYARROW_SCHEMA = pa.schema([
    pa.field("job_id",             pa.string(),  nullable=False),
    pa.field("device_id",          pa.string(),  nullable=False),
    pa.field("user_id",            pa.string(),  nullable=False),
    pa.field("circuit_depth",      pa.int32(),   nullable=False),
    pa.field("num_shots",          pa.int32(),   nullable=False),
    pa.field("queue_wait_seconds", pa.float64(), nullable=False),
    pa.field("exec_time_seconds",  pa.float64(), nullable=False),
    pa.field("total_time_seconds", pa.float64(), nullable=False),
    pa.field("status",             pa.string(),  nullable=False),
    pa.field("error_code",         pa.string(),  nullable=True),
    pa.field("sla_met",            pa.bool_(),   nullable=False),
    pa.field("timestamp",          pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_ingested_at",       pa.string(),  nullable=True),
    pa.field("_source_topic",      pa.string(),  nullable=True),
])


class JobsConsumer(BaseConsumer):
    def __init__(self, bronze_root=None, bootstrap_servers=None):
        schema = json.loads(SCHEMA_PATH.read_text())
        super().__init__(
            topic_key="jobs",
            topic="qpu.jobs",
            group_id="qpu-jobs-bronze-consumer",
            schema=schema,
            pyarrow_schema=PYARROW_SCHEMA,
            bronze_root=bronze_root,
            bootstrap_servers=bootstrap_servers,
        )

    def validate(self, record: Dict[str, Any]) -> bool:
        if not super().validate(record):
            return False

        expected_total = record.get("queue_wait_seconds", 0) + record.get("exec_time_seconds", 0)
        actual_total   = record.get("total_time_seconds", 0)
        if abs(actual_total - expected_total) > 0.05:
            logger.warning(
                "total_time mismatch on job_id=%s (expected=%.2f actual=%.2f)",
                record.get("job_id"), expected_total, actual_total,
            )
            return False

        status = record.get("status")
        error  = record.get("error_code")
        if status in ("completed", "cancelled") and error is not None:
            logger.warning("error_code set on %s job_id=%s", status, record.get("job_id"))
            return False

        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    JobsConsumer().run()