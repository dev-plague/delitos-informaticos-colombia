"""FASE 4 - Comparación MapReduce vs Spark sobre la misma zona raw.

Lee la salida del job de Hadoop Streaming (conteo por conducta, departamento,
año y total) y calcula exactamente lo mismo con Spark. Falla si los números
no coinciden.
"""
from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession, functions as F

from . import config, hdfs_client
from .common import escribir_json, log


def _leer_salida_mapreduce() -> dict[str, int]:
    rutas = [
        f for f in hdfs_client.listdir(f"{config.HDFS_MAPREDUCE}/conteo")
        if f["pathSuffix"].startswith("part-")
    ]
    resultado: dict[str, int] = {}
    for archivo in rutas:
        texto = hdfs_client.read_text(f"{config.HDFS_MAPREDUCE}/conteo/{archivo['pathSuffix']}")
        for linea in texto.splitlines():
            if "\t" not in linea:
                continue
            clave, valor = linea.split("\t", 1)
            resultado[clave] = resultado.get(clave, 0) + int(float(valor))
    return resultado


def _calcular_spark(spark: SparkSession) -> dict[str, int]:
    df = (
        spark.read.json(f"{config.HDFS_RAW}/*/*.jsonl")
        .select(
            F.trim(F.col("payload.descripcion_conducta")).alias("conducta"),
            F.trim(F.col("payload.departamento")).alias("departamento"),
            F.substring(F.col("payload.fecha_hecho"), 1, 4).alias("anio"),
            F.col("payload.cantidad").cast("int").alias("cantidad"),
        )
        .where(F.col("cantidad") > 0)
    )
    resultado: dict[str, int] = {}
    for campo, etiqueta in (("conducta", "CONDUCTA"), ("departamento", "DEPARTAMENTO"), ("anio", "ANIO")):
        for fila in df.groupBy(campo).agg(F.sum("cantidad").alias("casos")).collect():
            resultado[f"{etiqueta}|{fila[campo]}"] = int(fila["casos"])
    resultado["TOTAL|filas"] = df.count()
    return resultado


def main() -> int:
    spark = (
        SparkSession.builder.appName("delitos-comparacion-mapreduce")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        mr = _leer_salida_mapreduce()
        sp = _calcular_spark(spark)
        claves = sorted(set(mr) | set(sp))
        diferencias = [
            {"clave": k, "mapreduce": mr.get(k), "spark": sp.get(k)}
            for k in claves if mr.get(k) != sp.get(k)
        ]
        resumen = {
            "claves_comparadas": len(claves),
            "diferencias": len(diferencias),
            "coincide": not diferencias,
            "total_mapreduce": mr.get("TOTAL|filas"),
            "total_spark": sp.get("TOTAL|filas"),
            "detalle_diferencias": diferencias[:20],
        }
        escribir_json(os.path.join(config.EVIDENCIAS_DIR, "mapreduce_vs_spark.json"), resumen)

        lineas = [
            "# MapReduce vs Spark",
            "",
            "| Clave | MapReduce | Spark | ¿Coincide? |",
            "|---|---:|---:|---|",
        ]
        for clave in claves[:60]:
            lineas.append(
                f"| `{clave}` | {mr.get(clave)} | {sp.get(clave)} | "
                f"{'sí' if mr.get(clave) == sp.get(clave) else 'NO'} |"
            )
        lineas += [
            "",
            f"- Claves comparadas: **{len(claves)}**",
            f"- Diferencias: **{len(diferencias)}**",
            f"- Total de filas (MapReduce): **{mr.get('TOTAL|filas')}**",
            f"- Total de filas (Spark): **{sp.get('TOTAL|filas')}**",
        ]
        ruta_md = os.path.join(config.EVIDENCIAS_DIR, "mapreduce_vs_spark.md")
        with open(ruta_md, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lineas) + "\n")
        log(f"comparación escrita en {ruta_md}")
        log(f"claves={len(claves)} diferencias={len(diferencias)}")
        return 0 if not diferencias else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
