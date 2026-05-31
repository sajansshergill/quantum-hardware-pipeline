"""Create Kafka topics used by the local demo."""

from __future__ import annotations

import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPICS = [
    os.getenv("KAFKA_TOPIC_TELEMETRY", "qpu.telemetry"),
    os.getenv("KAFKA_TOPIC_CALIBRATION", "qpu.calibration"),
    os.getenv("KAFKA_TOPIC_JOBS", "qpu.jobs"),
    os.getenv("KAFKA_TOPIC_HEALTH", "qpu.health"),
]


def main() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        client_id="qpu-topic-bootstrap",
    )
    topics = [NewTopic(name=topic, num_partitions=4, replication_factor=1) for topic in TOPICS]
    try:
        admin.create_topics(topics)
        print("Created topics:", ", ".join(TOPICS))
    except TopicAlreadyExistsError:
        print("Topics already exist")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
