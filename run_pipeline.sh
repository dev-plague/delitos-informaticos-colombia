#!/usr/bin/env bash
# Pipeline completo: fuente -> raw -> Kafka -> HDFS -> Parquet -> calidad
#                   -> MapReduce vs Spark -> servicio -> tablero.
#
#   bash run_pipeline.sh              # todo, incluida la capa rápida
#   bash run_pipeline.sh --sin-streaming
set -euo pipefail
cd "$(dirname "$0")"

STREAMING=1
[[ "${1:-}" == "--sin-streaming" ]] && STREAMING=0

paso() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

paso "0/9 Levantando la plataforma (Kafka, HDFS, Spark, PostgreSQL, Streamlit)"
docker compose up -d --wait

paso "1/9 Tópicos de Kafka (3 particiones)"
docker compose exec -T spark python -m src.kafka_admin

paso "2/9 Ingesta a la zona raw (API Socrata, paginación y reintentos)"
docker compose exec -T spark python -m src.ingest

paso "3/9 Productor Kafka (key = id_registro)"
docker compose exec -T spark python -m src.kafka_producer --topic delitos-raw

paso "4/9 Consumidor Kafka -> HDFS (un archivo por lote)"
docker compose exec -T spark python -m src.kafka_consumer_hdfs --inactividad 15

paso "5/9 Zona curada con PySpark (Parquet particionado por año)"
docker compose exec -T spark python -m src.spark_curated

paso "6/9 Validaciones de calidad (detiene el pipeline si falla una crítica)"
docker compose exec -T spark python -m src.quality

paso "7/9 MapReduce sobre HDFS y comparación con Spark"
bash scripts/run_mapreduce.sh
docker compose exec -T spark python -m src.compare_mapreduce

paso "8/9 Zona de servicio en PostgreSQL"
docker compose exec -T spark python -m src.load_service

if [[ "$STREAMING" == "1" ]]; then
  paso "9/9 Capa rápida: Structured Streaming sobre Kafka"
  docker compose exec -T spark python -m src.kafka_producer --topic delitos-stream --dias 90
  bash scripts/run_streaming_demo.sh
fi

docker compose exec -T spark python -m src.register_run --etiqueta "pipeline-completo"

paso "Listo"
echo "Tablero:  http://localhost:8501"
echo "Jupyter:  http://localhost:8888"
echo "NameNode: http://localhost:9870"
