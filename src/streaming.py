"""FASE 7 - Capa rápida con Spark Structured Streaming (arquitectura Lambda).

Lee el tópico `delitos-stream` (últimos 90 días), agrega casos por ventana
diaria y conducta con watermark y checkpoint, y escribe el resultado en
PostgreSQL (`service.stream_diario`) con upsert.

El checkpoint vive en HDFS: al reiniciar, el flujo continúa desde los offsets
confirmados y el upsert evita duplicar filas.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime

import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import config, hdfs_client
from .common import escribir_json, log, pg_connect

ESQUEMA = T.StructType(
    [
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("fecha_hecho", T.StringType()),
                    T.StructField("descripcion_conducta", T.StringType()),
                    T.StructField("cantidad", T.StringType()),
                ]
            ),
        )
    ]
)


def _upsert(batch_df, epoch_id: int) -> None:
    filas = batch_df.collect()
    if not filas:
        return
    registros = []
    for r in filas:
        if r["fecha"] is None:
            continue
        registros.append((r["fecha"], r["codigo"], r["conducta"], int(r["casos"])))
    if not registros:
        return
    with pg_connect() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO service.stream_diario (fecha, codigo, conducta, casos)
            VALUES %s
            ON CONFLICT (fecha, codigo) DO UPDATE SET
                casos = EXCLUDED.casos,
                conducta = EXCLUDED.conducta,
                actualizado = now()
            """,
            registros,
            page_size=1000,
        )
        conn.commit()
    log(f"  batch {epoch_id}: {len(registros)} filas agregadas -> PostgreSQL")


def ejecutar(duracion: int = 60, reset: bool = False) -> dict:
    checkpoint = f"{config.HDFS_CHECKPOINTS}/stream_diario"
    if reset and hdfs_client.exists(checkpoint):
        log("borrando checkpoint anterior")
        hdfs_client.delete_recursive(checkpoint)

    spark = (
        SparkSession.builder.appName("delitos-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TOPIC_STREAM)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )

    eventos = (
        crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("sobre"))
        .select(
            F.to_timestamp("sobre.payload.fecha_hecho").alias("evento_ts"),
            F.to_date(F.to_timestamp("sobre.payload.fecha_hecho")).alias("fecha"),
            F.regexp_extract(F.col("sobre.payload.descripcion_conducta"), r"ARTICULO\s+(269[A-J])", 1).alias("codigo"),
            F.trim(F.col("sobre.payload.descripcion_conducta")).alias("conducta"),
            F.col("sobre.payload.cantidad").cast("int").alias("cantidad"),
        )
        .where(F.col("fecha").isNotNull() & (F.col("cantidad") > 0))
        # La capa rápida cubre 90 días; el watermark de 120 días tolera
        # eventos tardíos sin descartar la ventana completa.
        .withWatermark("evento_ts", "120 days")
    )

    agregado = (
        eventos.groupBy(F.window("evento_ts", "1 day").alias("ventana"), "codigo", "conducta")
        .agg(F.sum("cantidad").alias("casos"))
        .select(
            F.col("ventana.start").cast("date").alias("fecha"),
            "codigo",
            "conducta",
            "casos",
        )
    )

    consulta = (
        agregado.writeStream.outputMode("update")
        .foreachBatch(_upsert)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )
    log(f"flujo iniciado ({duracion}s); checkpoint={checkpoint}")

    consulta.awaitTermination(timeout=duracion)
    activo = consulta.isActive
    if activo:
        consulta.stop()
    ultimo = consulta.lastProgress or {}
    offsets = (ultimo.get("sources") or [{}])[0].get("endOffset") if ultimo.get("sources") else None

    spark.stop()

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(casos),0), min(fecha), max(fecha) FROM service.stream_diario")
        filas, casos, minima, maxima = cur.fetchone()

    resultado = {
        "inicio": datetime.now().isoformat(timespec="seconds"),
        "duracion_seg": duracion,
        "detenido_por_tiempo": activo,
        "checkpoint": checkpoint,
        "offset_final": offsets,
        "filas_stream": filas,
        "casos_stream": int(casos),
        "fecha_min": str(minima),
        "fecha_max": str(maxima),
    }
    escribir_json(
        os.path.join(config.EVIDENCIAS_DIR, f"streaming_{datetime.now():%Y%m%dT%H%M%S}.json"),
        resultado,
    )
    log(f"FIN streaming: {filas} filas, {casos} casos, offset={offsets}")
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(description="Structured Streaming sobre Kafka")
    parser.add_argument("--duracion", type=int, default=60, help="Segundos que corre el flujo")
    parser.add_argument("--reset", action="store_true", help="Borra el checkpoint antes de iniciar")
    args = parser.parse_args()
    print(json.dumps(ejecutar(args.duracion, args.reset), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
