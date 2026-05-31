"""
Reusable Kafka producer for synthetic QPU event streams.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable, Iterable, Mapping, Tuple

from kafka import KafkaProducer

logger = logging.getLogger(__name__)

TOPIC_ENV_KEYS = {
    "telemetry": "KAFKA_TOPIC_TELEMETRY",
    "calibration": "KAFKA_TOPIC_CALIBRATION",
    "jobs": "KAFKA_TOPIC_JOBS",
    "health": "KAFKA_TOPIC_HEALTH",
}

DEFAULT_TOPICS = {
    "telemetry": "qpu.telemetry",
    "calibration": "qpu.calibration",
    "jobs": "qpu.jobs",
    "health": "qpu.health",
}


def topic_for(topic_key: str) -> str:
    """Resolve the Kafka topic name for a logical stream."""
    if topic_key not in DEFAULT_TOPICS:
        raise ValueError(f"Unknown topic key {topic_key!r}; expected one of {sorted(DEFAULT_TOPICS)}")
    return os.getenv(TOPIC_ENV_KEYS[topic_key], DEFAULT_TOPICS[topic_key])


class BaseProducer:
    """Small wrapper around KafkaProducer with project defaults."""

    def __init__(self, topic_key: str, bootstrap_servers: str | None = None) -> None:
        self.topic_key = topic_key
        self.topic = topic_for(topic_key)
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),
            retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", "5")),
        )

    def send_batch(self, batch: Iterable[Tuple[Mapping, str]]) -> int:
        """Send a batch of ``(record, partition_key)`` tuples and flush once."""
        sent = 0
        for record, partition_key in batch:
            self.producer.send(self.topic, key=partition_key, value=dict(record))
            sent += 1
        self.producer.flush()
        logger.info("Published %d %s records to %s", sent, self.topic_key, self.topic)
        return sent

    def run_loop(
        self,
        batch_factory: Callable[[], Iterable[Tuple[Mapping, str]]],
        interval_seconds: float,
    ) -> None:
        """Continuously publish batches until interrupted."""
        logger.info(
            "Starting %s producer on topic=%s bootstrap=%s interval=%.1fs",
            self.topic_key,
            self.topic,
            self.bootstrap_servers,
            interval_seconds,
        )
        try:
            while True:
                self.send_batch(batch_factory())
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Stopping %s producer", self.topic_key)
        finally:
            self.producer.close()
