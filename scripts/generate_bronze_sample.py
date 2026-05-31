"""Generate a small bronze Parquet dataset without requiring Kafka."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT / "ingestion" / "consumers"))

from calibration_consumer import CalibrationConsumer
from calibration_producer import generate_calibration_batch
from health_consumer import HealthConsumer
from health_producer import generate_health_batch
from jobs_consumer import JobsConsumer
from jobs_producer import generate_jobs_batch
from telemetry_consumer import TelemetryConsumer
from telemetry_producer import generate_telemetry_batch


def _records(batch):
    return [record for record, _ in batch]


def main() -> None:
    (ROOT / "data" / "lakehouse").mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "alerts").mkdir(parents=True, exist_ok=True)
    consumers_and_records = [
        (TelemetryConsumer(), _records(generate_telemetry_batch())),
        (CalibrationConsumer(), _records(generate_calibration_batch())),
        (JobsConsumer(), _records(generate_jobs_batch(jobs_per_device=8))),
        (HealthConsumer(), _records(generate_health_batch())),
    ]

    total = 0
    for consumer, records in consumers_and_records:
        valid = [record for record in records if consumer.validate(record)]
        total += consumer.write_records(valid)
    print(json.dumps({"bronze_root": "data/bronze", "records_written": total}, indent=2))


if __name__ == "__main__":
    main()
