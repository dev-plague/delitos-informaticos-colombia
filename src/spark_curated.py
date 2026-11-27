"""FASE 5 - Zona curada con PySpark.

Lee la zona raw de HDFS, elimina duplicados por id_registro, corrige tipos,
nulos y formatos, y guarda Parquet particionado por año. Deja un reporte con
el antes y el después de cada decisión y la comparación de tamaños.
"""
from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import config, hdfs_client
from .common import asegurar_dir, escribir_json, log

RAW_SCHEMA = T.StructType(
    [
        T.StructField("fuente", T.StringType()),
        T.StructField("dataset", T.StringType()),
        T.StructField("id_registro", T.StringType()),
        T.StructField("id_fuente", T.StringType()),
        T.StructField("hash_contenido", T.StringType()),
        T.StructField("fecha_ingesta", T.StringType()),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("fecha_hecho", T.StringType()),
                    T.StructField("cod_depto", T.StringType()),
                    T.StructField("departamento", T.StringType()),
                    T.StructField("cod_muni", T.StringType()),
                    T.StructField("municipio", T.StringType()),
                    T.StructField("descripcion_conducta", T.StringType()),
                    T.StructField("cantidad", T.StringType()),
                ]
            ),
        ),
    ]
)


def _spark() -> SparkSession:
    return (
        SparkSession.builder.appName("delitos-zona-curada")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _tamano_local(path: str) -> int:
    total = 0
    for raiz, _dirs, archivos in os.walk(path):
        for archivo in archivos:
            total += os.path.getsize(os.path.join(raiz, archivo))
    return total


def curar(spark: SparkSession) -> dict:
    log("leyendo zona raw de HDFS")
    crudo = spark.read.schema(RAW_SCHEMA).json(f"{config.HDFS_RAW}/*/*.jsonl")

    base = crudo.select(
        F.col("id_registro"),
        F.col("hash_contenido"),
        F.to_date(F.to_timestamp("payload.fecha_hecho")).alias("fecha_hecho"),
        F.to_timestamp("fecha_ingesta").alias("fecha_ingesta"),
        F.lpad(F.trim("payload.cod_depto"), 2, "0").alias("cod_depto"),
        F.upper(F.trim("payload.departamento")).alias("departamento"),
        F.lpad(F.trim("payload.cod_muni"), 5, "0").alias("cod_muni"),
        F.upper(F.trim("payload.municipio")).alias("municipio"),
        F.regexp_replace(F.trim("payload.descripcion_conducta"), r"\s+", " ").alias("conducta"),
        F.col("payload.cantidad").cast("int").alias("cantidad"),
    ).cache()

    # 1) Nulos en columnas clave: se descartan (no hay forma de imputar una
    #    fecha o un municipio sin inventar el hecho).
    clave_completa = (
        F.col("id_registro").isNotNull()
        & F.col("fecha_hecho").isNotNull()
        & F.col("cod_muni").isNotNull()
        & F.col("conducta").isNotNull()
        & (F.length("conducta") > 0)
        & F.col("cantidad").isNotNull()
    )
    filas_entrada = base.count()
    sin_nulos = base.filter(clave_completa)
    nulos_eliminados = filas_entrada - sin_nulos.count()

    # 2) Duplicados por id_registro: la fuente no tiene id propio, así que se
    #    conserva la primera aparición del hecho.
    dedup = sin_nulos.dropDuplicates(["id_registro"])
    duplicados = sin_nulos.count() - dedup.count()

    # Filas con el mismo contenido (fecha, municipio, conducta y cantidad):
    # en este dataset son casos distintos (p. ej. dos víctimas el mismo día),
    # por eso NO se descartan; se reportan para dejar constancia. Se cuentan
    # sobre los registros únicos, no sobre los duplicados del pipeline.
    contenido_repetido = dedup.count() - dedup.select("hash_contenido").distinct().count()

    # 3) Cantidad: debe ser un entero positivo y razonable para un registro
    #    agregado (más de 1.000 casos en un municipio/día/conducta es un error).
    valido_cantidad = dedup.filter((F.col("cantidad") > 0) & (F.col("cantidad") <= 1000))
    cantidad_invalida = dedup.count() - valido_cantidad.count()

    # 4) Fechas: entre 2000-01-01 y hoy.
    valido_fecha = valido_cantidad.filter(
        (F.col("fecha_hecho") >= F.lit("2000-01-01")) & (F.col("fecha_hecho") <= F.current_date())
    )
    fechas_invalidas = valido_cantidad.count() - valido_fecha.count()

    # 5) Códigos de departamento/municipio con la longitud oficial (2 y 5).
    valido_codigo = valido_fecha.filter(
        (F.length("cod_depto") == 2) & (F.length("cod_muni") == 5)
    )
    codigos_invalidos = valido_fecha.count() - valido_codigo.count()

    limpio = (
        valido_codigo.withColumn("codigo", F.regexp_extract("conducta", r"ARTICULO\s+(269[A-J])", 1))
        .withColumn("anio", F.year("fecha_hecho"))
        .withColumn("mes", F.month("fecha_hecho"))
        .withColumn("anio_mes", F.date_format("fecha_hecho", "yyyy-MM"))
        .select(
            "id_registro",
            "hash_contenido",
            "fecha_hecho",
            "fecha_ingesta",
            "anio",
            "mes",
            "anio_mes",
            "cod_depto",
            "departamento",
            "cod_muni",
            "municipio",
            "codigo",
            "conducta",
            "cantidad",
        )
    )

    filas_salida = limpio.count()
    log(f"filas entrada={filas_entrada} salida={filas_salida}")

    log("escribiendo Parquet particionado por anio en HDFS")
    limpio.repartition("anio").write.mode("overwrite").partitionBy("anio").parquet(config.HDFS_CURATED)

    # Muestra pequeña y versionable para el repositorio y el tablero
    asegurar_dir(config.DATA_DIR)
    muestra_pdf = limpio.orderBy(F.desc("fecha_hecho")).limit(2000).toPandas()
    ruta_muestra = os.path.join(config.DATA_DIR, "curated_sample.parquet")
    muestra_pdf.to_parquet(ruta_muestra, index=False)

    tamano_raw = hdfs_client.du(config.HDFS_RAW)
    tamano_parquet = hdfs_client.du(config.HDFS_CURATED)
    tamano_local_jsonl = _tamano_local(config.RAW_DIR)

    metricas = {
        "filas_entrada": filas_entrada,
        "filas_salida": filas_salida,
        "descartados": filas_entrada - filas_salida,
        "duplicados_id_registro": duplicados,
        "filas_con_mismo_contenido_conservadas": contenido_repetido,
        "nulos_columna_clave": nulos_eliminados,
        "cantidad_invalida": cantidad_invalida,
        "fecha_invalida": fechas_invalidas,
        "codigo_invalido": codigos_invalidos,
        "particion": "anio",
        "tamanos_bytes": {
            "raw_hdfs_jsonl": tamano_raw,
            "curado_hdfs_parquet": tamano_parquet,
            "raw_local_jsonl": tamano_local_jsonl,
        },
        "reduccion_parquet_pct": round(100 * (1 - tamano_parquet / tamano_raw), 2) if tamano_raw else None,
    }
    escribir_json(os.path.join(config.EVIDENCIAS_DIR, "curated_metrics.json"), metricas)

    lineas = [
        "# Zona curada: antes y después",
        "",
        "| Problema | Filas afectadas | Decisión |",
        "|---|---:|---|",
        f"| Nulos en columnas clave | {nulos_eliminados} | Se descartan: no se puede reconstruir el hecho |",
        f"| Duplicados por `id_registro` | {duplicados} | Se conserva la primera aparición |",
        f"| `cantidad` fuera de rango | {cantidad_invalida} | Se descartan valores <= 0 o > 1000 |",
        f"| Fechas fuera de rango | {fechas_invalidas} | Se descartan fechas < 2000 o futuras |",
        f"| Códigos DANE inválidos | {codigos_invalidos} | Se exige longitud 2 (depto) y 5 (municipio) |",
        "",
        f"**Filas con contenido idéntico conservadas:** {contenido_repetido:,}. No son "
        "duplicados del pipeline: el dataset publica un registro por caso y dos casos "
        "pueden coincidir en fecha, municipio, conducta y cantidad. El `id_registro` "
        "(`:id` de Socrata) los distingue.",
        "",
        f"**Filas de entrada:** {filas_entrada:,} → **filas curadas:** {filas_salida:,} "
        f"(descartadas {filas_entrada - filas_salida:,}).",
        "",
        "## Tamaño JSONL vs Parquet",
        "",
        "| Zona | Bytes |",
        "|---|---:|",
        f"| Raw HDFS (JSONL) | {tamano_raw:,} |",
        f"| Curado HDFS (Parquet) | {tamano_parquet:,} |",
        f"| Reducción | {metricas['reduccion_parquet_pct']}% |",
        "",
        "**Partición elegida:** `anio`. El tablero y las validaciones filtran por año, "
        "la cardinalidad es baja (4 carpetas) y evita el problema de miles de carpetas "
        "que tendría particionar por municipio (1.018 valores).",
    ]
    with open(os.path.join(config.EVIDENCIAS_DIR, "curated_metrics.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lineas) + "\n")

    base.unpersist()
    return metricas


def main() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        metricas = curar(spark)
        log(json.dumps(metricas, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
