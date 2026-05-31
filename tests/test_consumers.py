import sys
from pathlib import Path

import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT / "ingestion" / "consumers"))

from jobs_consumer import JobsConsumer
from jobs_producer import generate_jobs_batch
from telemetry_consumer import TelemetryConsumer
from telemetry_producer import generate_telemetry_batch


def test_telemetry_consumer_rejects_domain_violations():
    consumer = TelemetryConsumer()
    record = generate_telemetry_batch()[0][0]

    assert consumer.validate(record)

    bad_t2 = dict(record, T2_us=record["T1_us"] * 2 + 1)
    assert not consumer.validate(bad_t2)

    bad_gate_error = dict(record, gate_error_2q=record["gate_error_1q"] / 2)
    assert not consumer.validate(bad_gate_error)


def test_jobs_consumer_writes_partitioned_parquet(tmp_path):
    consumer = JobsConsumer(bronze_root=tmp_path)
    records = [record for record, _ in generate_jobs_batch(jobs_per_device=1)]

    assert all(consumer.validate(record) for record in records)
    assert consumer.write_records(records) == len(records)

    dataset = ds.dataset(tmp_path / "jobs", format="parquet", partitioning="hive")
    table = dataset.to_table()
    assert table.num_rows == len(records)
    assert "date" in table.column_names
    assert "device_id" in table.column_names
