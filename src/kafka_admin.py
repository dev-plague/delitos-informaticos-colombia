"""FASE 3 - Administración del tópico de Kafka (3 particiones)."""
from __future__ import annotations

import argparse

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from . import config
from .common import log


def crear_topic(nombre: str, particiones: int = config.TOPIC_PARTITIONS) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        client_id="delitos-admin",
        request_timeout_ms=30000,
    )
    try:
        admin.create_topics(
            [NewTopic(name=nombre, num_partitions=particiones, replication_factor=1)]
        )
        log(f"tópico '{nombre}' creado con {particiones} particiones")
    except TopicAlreadyExistsError:
        log(f"tópico '{nombre}' ya existe (se conserva)")
    finally:
        admin.close()


def describir(nombre: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP, client_id="delitos-admin")
    try:
        info = admin.describe_topics([nombre])[0]
        log(f"tópico {nombre}: {len(info['partitions'])} particiones")
        for p in info["partitions"]:
            log(f"  partición {p['partition']} -> líder {p['leader']} réplicas {p['replicas']}")
    finally:
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Crea los tópicos del pipeline")
    parser.add_argument("--topic", action="append", help="Tópico a crear (repetible)")
    parser.add_argument("--particiones", type=int, default=config.TOPIC_PARTITIONS)
    args = parser.parse_args()
    for topic in args.topic or [config.TOPIC_RAW, config.TOPIC_STREAM]:
        crear_topic(topic, args.particiones)
        describir(topic)


if __name__ == "__main__":
    main()
