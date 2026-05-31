"""
Shared Kafka-to-bronze consumer implementation.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyarrow as pa
import pyarrow.dataset as ds
from jsonschema import ValidationError, validate as validate_json
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class BaseConsumer:
    """Validate Kafka records and write them as hive-partitioned bronze Parquet."""

    def __init__(
        self,
        topic_key: str,
        topic: str,
        group_id: str,
        schema: Dict[str, Any],
        pyarrow_schema: pa.Schema,
        bronze_root: str | Path | None = None,
        bootstrap_servers: str | None = None,
    ) -> None:
        self.topic_key = topic_key
        self.topic = topic
        self.group_id = group_id
        self.schema = schema
        self.pyarrow_schema = pyarrow_schema
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.bronze_root = Path(bronze_root or os.getenv("BRONZE_ROOT", "data/bronze"))
        self.poll_timeout_ms = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))
        self.batch_size = int(os.getenv("CONSUMER_BATCH_SIZE", "500"))

    def validate(self, record: Dict[str, Any]) -> bool:
        """Run JSON Schema validation. Subclasses add domain checks."""
        try:
            validate_json(instance=record, schema=self.schema)
            return True
        except ValidationError as exc:
            logger.warning("Schema validation failed for topic=%s: %s", self.topic, exc.message)
            return False

    def enrich(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        event_time = _parse_timestamp(enriched["timestamp"])
        enriched["timestamp"] = event_time
        enriched["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        enriched["_source_topic"] = self.topic
        enriched["date"] = event_time.date().isoformat()
        return enriched

    def write_records(self, records: Iterable[Dict[str, Any]]) -> int:
        rows = [self.enrich(record) for record in records]
        if not rows:
            return 0

        schema_with_partitions = self.pyarrow_schema.append(pa.field("date", pa.string(), nullable=False))
        table = pa.Table.from_pylist(rows, schema=schema_with_partitions)
        target = self.bronze_root / self.topic_key
        target.mkdir(parents=True, exist_ok=True)
        ds.write_dataset(
            table,
            base_dir=str(target),
            format="parquet",
            basename_template=f"batch-{uuid.uuid4().hex}-{{i}}.parquet",
            partitioning=ds.partitioning(
                pa.schema([("date", pa.string()), ("device_id", pa.string())]),
                flavor="hive",
            ),
            existing_data_behavior="overwrite_or_ignore",
        )
        logger.info("Wrote %d %s records to %s", len(rows), self.topic_key, target)
        return len(rows)

    def consume_once(self, max_records: int | None = None) -> int:
        consumer = self._consumer()
        try:
            messages = consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=max_records or self.batch_size)
            records: List[Dict[str, Any]] = []
            for partition_messages in messages.values():
                for message in partition_messages:
                    if self.validate(message.value):
                        records.append(message.value)
            written = self.write_records(records)
            if written:
                consumer.commit()
            return written
        finally:
            consumer.close()

    def run(self) -> None:
        consumer = self._consumer()
        buffer: List[Dict[str, Any]] = []
        logger.info("Consuming %s into %s", self.topic, self.bronze_root / self.topic_key)
        try:
            for message in consumer:
                if self.validate(message.value):
                    buffer.append(message.value)
                if len(buffer) >= self.batch_size:
                    self.write_records(buffer)
                    consumer.commit()
                    buffer.clear()
        except KeyboardInterrupt:
            logger.info("Stopping %s consumer", self.topic_key)
        finally:
            if buffer:
                self.write_records(buffer)
                consumer.commit()
            consumer.close()

    def _consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
