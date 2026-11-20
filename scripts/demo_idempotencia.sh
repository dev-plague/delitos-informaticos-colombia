#!/usr/bin/env bash
# FASE 6 - Demostración de idempotencia: corre la parte batch otra vez y
# compara los conteos con la corrida anterior.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[idempotencia] ingesta incremental (marca de agua = última fecha cargada)"
docker compose exec -T spark python -m src.ingest | tail -6

echo "[idempotencia] productor y consumidor (sólo los registros nuevos)"
docker compose exec -T spark python -m src.kafka_producer --topic delitos-raw | tail -2
docker compose exec -T spark python -m src.kafka_consumer_hdfs --inactividad 12 | tail -2

echo "[idempotencia] curaduría (elimina duplicados por id_registro)"
docker compose exec -T spark python -m src.spark_curated | tail -20

echo "[idempotencia] calidad"
docker compose exec -T spark python -m src.quality | tail -5

echo "[idempotencia] recarga de la zona de servicio"
docker compose exec -T spark python -m src.load_service | tail -3

docker compose exec -T spark python -m src.register_run --etiqueta "segunda-corrida-idempotencia"

echo
echo "[idempotencia] historial de corridas:"
cat evidencias/idempotencia_corridas.json
