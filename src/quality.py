"""FASE 6 - Validaciones automáticas de calidad.

Lee la zona curada, ejecuta 7 validaciones y escribe un reporte con los
registros descartados. Si una validación CRÍTICA falla, el proceso termina
con código 2 para que el orquestador detenga el pipeline.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import config
from .common import escribir_json, log


def _cargar_metricas() -> dict:
    ruta = os.path.join(config.EVIDENCIAS_DIR, "curated_metrics.json")
    if os.path.exists(ruta):
        with open(ruta, encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def validar(spark: SparkSession) -> dict:
    df = spark.read.parquet(config.HDFS_CURATED).cache()
    filas = df.count()

    def contar(condicion) -> int:
        return df.filter(condicion).count()

    checks = [
        {
            "nombre": "conteo_mayor_cero",
            "descripcion": "La zona curada tiene al menos un registro",
            "critico": True,
            "fallos": 0 if filas > 0 else 1,
            "valor": filas,
        },
        {
            "nombre": "sin_nulos_columnas_clave",
            "descripcion": "Sin nulos en fecha_hecho, cod_muni, conducta y cantidad",
            "critico": True,
            "fallos": contar(
                F.col("fecha_hecho").isNull()
                | F.col("cod_muni").isNull()
                | F.col("conducta").isNull()
                | F.col("cantidad").isNull()
            ),
            "valor": filas,
        },
        {
            "nombre": "sin_id_registro_repetido",
            "descripcion": "id_registro único en toda la zona curada",
            "critico": True,
            "fallos": filas - df.select("id_registro").distinct().count(),
            "valor": filas,
        },
        {
            "nombre": "cantidad_en_rango",
            "descripcion": "cantidad entre 1 y 1000",
            "critico": False,
            "fallos": contar(~F.col("cantidad").between(1, 1000)),
            "valor": filas,
        },
        {
            "nombre": "fechas_en_rango",
            "descripcion": "fecha_hecho entre 2000-01-01 y hoy",
            "critico": False,
            "fallos": contar(
                (F.col("fecha_hecho") < F.lit("2000-01-01")) | (F.col("fecha_hecho") > F.current_date())
            ),
            "valor": filas,
        },
        {
            "nombre": "codigos_dane_validos",
            "descripcion": "cod_depto de 2 dígitos y cod_muni de 5",
            "critico": False,
            "fallos": contar((F.length("cod_depto") != 2) | (F.length("cod_muni") != 5)),
            "valor": filas,
        },
        {
            "nombre": "sin_conductas_vacias",
            "descripcion": "Toda fila tiene descripción de conducta y código 269x",
            "critico": False,
            "fallos": contar((F.length("conducta") == 0) | (F.col("codigo") == "")),
            "valor": filas,
        },
    ]

    metricas = _cargar_metricas()
    criticos_fallidos = [c for c in checks if c["critico"] and c["fallos"] > 0]
    reporte = {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "filas_curadas": filas,
        "registros_descartados_en_curacion": metricas.get("descartados"),
        "checks": checks,
        "criticos_fallidos": [c["nombre"] for c in criticos_fallidos],
        "estado": "fallido" if criticos_fallidos else "ok",
    }
    escribir_json(os.path.join(config.EVIDENCIAS_DIR, "calidad_reporte.json"), reporte)

    lineas = [
        "# Reporte de calidad",
        "",
        f"- Filas curadas: **{filas:,}**",
        f"- Registros descartados en la curaduría: **{metricas.get('descartados')}**",
        f"- Estado: **{reporte['estado'].upper()}**",
        "",
        "| Validación | Crítica | Fallos | Descripción |",
        "|---|---|---:|---|",
    ]
    for c in checks:
        lineas.append(
            f"| `{c['nombre']}` | {'sí' if c['critico'] else 'no'} | {c['fallos']} | {c['descripcion']} |"
        )
    with open(os.path.join(config.EVIDENCIAS_DIR, "calidad_reporte.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lineas) + "\n")

    df.unpersist()
    log(f"calidad: {reporte['estado']} ({len(criticos_fallidos)} críticas fallidas)")
    return reporte


def main() -> int:
    spark = (
        SparkSession.builder.appName("delitos-calidad")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        reporte = validar(spark)
        print(json.dumps(reporte, indent=2, ensure_ascii=False))
        if reporte["estado"] != "ok":
            print("PIPELINE DETENIDO: falló una validación crítica", file=sys.stderr)
            return 2
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
