#!/usr/bin/env bash
# FASE 4 - Job de MapReduce (Hadoop Streaming) sobre la zona raw en HDFS.
# Se ejecuta desde el host:  bash scripts/run_mapreduce.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[mapreduce] copiando mapper y reducer al contenedor del namenode"
docker compose cp src/mapreduce/mapper.py namenode:/tmp/mapper.py
docker compose cp src/mapreduce/reducer.py namenode:/tmp/reducer.py

echo "[mapreduce] limpiando salida anterior"
docker compose exec -T namenode bash -lc \
  'hdfs dfs -rm -r -f /data/delitos_informaticos/mapreduce/conteo >/dev/null 2>&1 || true'

echo "[mapreduce] ejecutando Hadoop Streaming"
docker compose exec -T namenode bash -lc '
  hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.job.name="delitos-conteo-raw" \
    -D mapreduce.job.reduces=1 \
    -files /tmp/mapper.py,/tmp/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "/data/delitos_informaticos/raw/fecha_ingesta=*/*.jsonl" \
    -output /data/delitos_informaticos/mapreduce/conteo
'

echo "[mapreduce] resultado (primeras líneas)"
docker compose exec -T namenode bash -lc \
  'hdfs dfs -cat /data/delitos_informaticos/mapreduce/conteo/part-* | sort | head -20'
