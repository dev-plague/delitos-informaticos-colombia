"""FASE 8 - Zona de servicio en PostgreSQL.

Calcula los indicadores con Spark (una sola vez) y los deja listos para el
tablero: el tablero sólo consulta, no calcula. La carga es transaccional
(TRUNCATE + INSERT), por lo que repetirla deja exactamente el mismo estado.
"""
from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
import psycopg2.extras
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from . import config
from .common import guardar_watermark, log, pg_connect

TABLAS = [
    "service.ind_casos_anio",
    "service.ind_conducta_anio",
    "service.ind_departamento_anio",
    "service.ind_municipio",
    "service.ind_mensual",
    "service.fct_delitos",
    "service.ind_kpis",
]


def _py(valor):
    """Normaliza tipos de pandas/numpy para psycopg2."""
    if valor is None:
        return None
    if isinstance(valor, float) and pd.isna(valor):
        return None
    if valor is pd.NaT:
        return None
    if hasattr(valor, "item") and not isinstance(valor, (str, bytes)):
        try:
            valor = valor.item()
        except Exception:
            pass
    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()
    if hasattr(valor, "to_pydatetime"):
        return valor.to_pydatetime()
    if isinstance(valor, float) and valor.is_integer():
        return valor
    return valor


def _insertar(cur, tabla: str, columnas: list[str], filas: list[tuple], conflicto: str = "DO NOTHING") -> int:
    if not filas:
        return 0
    plantilla = "(" + ", ".join(["%s"] * len(columnas)) + ")"
    consulta = (
        f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES %s "
        f"ON CONFLICT {conflicto}"
    )
    psycopg2.extras.execute_values(cur, consulta, filas, page_size=5000)
    return len(filas)


def _filas(df, columnas: list[str]) -> list[tuple]:
    return [tuple(_py(v) for v in fila) for fila in df.select(*columnas).toPandas().itertuples(index=False, name=None)]


def cargar(spark: SparkSession) -> dict:
    curado = spark.read.parquet(config.HDFS_CURATED).cache()
    total_casos, total_registros = curado.agg(
        F.sum("cantidad").alias("casos"), F.count("*").alias("registros")
    ).first()

    por_anio = (
        curado.groupBy("anio")
        .agg(F.sum("cantidad").alias("casos"), F.count("*").alias("registros"))
        .withColumn("participacion", F.round(F.col("casos") / F.lit(total_casos), 4))
        .orderBy("anio")
    )

    conducta_anio = (
        curado.groupBy("anio", "codigo", "conducta")
        .agg(F.sum("cantidad").alias("casos"))
        .orderBy("anio", F.desc("casos"))
    )

    depto_anio = (
        curado.groupBy("anio", "departamento")
        .agg(F.sum("cantidad").alias("casos"))
        .withColumn(
            "ranking",
            F.row_number().over(Window.partitionBy("anio").orderBy(F.desc("casos"), "departamento")),
        )
        .orderBy("anio", "ranking")
    )

    municipio = (
        curado.groupBy("departamento", "municipio")
        .agg(F.sum("cantidad").alias("casos"))
        .withColumn(
            "ranking",
            F.row_number().over(Window.orderBy(F.desc("casos"), "departamento", "municipio")),
        )
        .filter(F.col("ranking") <= 30)
    )

    mensual = (
        curado.groupBy("anio_mes")
        .agg(F.sum("cantidad").alias("casos"), F.count("*").alias("registros"))
        .orderBy("anio_mes")
    )

    conducta_top = (
        curado.groupBy("codigo", "conducta").agg(F.sum("cantidad").alias("casos"))
        .orderBy(F.desc("casos")).first()
    )
    depto_top = (
        curado.groupBy("departamento").agg(F.sum("cantidad").alias("casos"))
        .orderBy(F.desc("casos")).first()
    )
    anio_top = por_anio.orderBy(F.desc("casos")).first()
    fecha_corte = curado.agg(F.max("fecha_hecho")).first()[0]

    kpis = pd.DataFrame(
        [
            {
                "id": 1,
                "total_casos": int(total_casos),
                "total_registros": int(total_registros),
                "conducta_top": f"{conducta_top['codigo']} - {conducta_top['conducta']}",
                "casos_conducta_top": int(conducta_top["casos"]),
                "depto_top": depto_top["departamento"],
                "casos_depto_top": int(depto_top["casos"]),
                "anio_pico": int(anio_top["anio"]),
                "casos_anio_pico": int(anio_top["casos"]),
                "fecha_corte": fecha_corte,
                "actualizado": datetime.now(),
            }
        ]
    )

    log(f"cargando indicadores en PostgreSQL ({total_registros} registros, {total_casos} casos)")
    with pg_connect() as conn, conn.cursor() as cur:
        for tabla in TABLAS:
            cur.execute(f"TRUNCATE {tabla}")

        _insertar(cur, "service.ind_casos_anio", ["anio", "casos", "registros", "participacion"],
                  _filas(por_anio, ["anio", "casos", "registros", "participacion"]))
        _insertar(
            cur,
            "service.ind_conducta_anio",
            ["anio", "codigo", "conducta", "casos"],
            _filas(conducta_anio, ["anio", "codigo", "conducta", "casos"]),
            conflicto="(anio, codigo) DO UPDATE SET casos=EXCLUDED.casos, conducta=EXCLUDED.conducta",
        )
        _insertar(
            cur,
            "service.ind_departamento_anio",
            ["anio", "departamento", "casos", "ranking"],
            _filas(depto_anio, ["anio", "departamento", "casos", "ranking"]),
            conflicto="(anio, departamento) DO UPDATE SET casos=EXCLUDED.casos, ranking=EXCLUDED.ranking",
        )
        _insertar(
            cur,
            "service.ind_municipio",
            ["municipio", "departamento", "casos", "ranking"],
            _filas(municipio, ["municipio", "departamento", "casos", "ranking"]),
            conflicto="(departamento, municipio) DO UPDATE SET casos=EXCLUDED.casos, ranking=EXCLUDED.ranking",
        )
        _insertar(
            cur,
            "service.ind_mensual",
            ["anio_mes", "casos", "registros"],
            _filas(mensual, ["anio_mes", "casos", "registros"]),
            conflicto="(anio_mes) DO UPDATE SET casos=EXCLUDED.casos, registros=EXCLUDED.registros",
        )
        _insertar(cur, "service.ind_kpis", list(kpis.columns), [tuple(_py(v) for v in kpis.iloc[0].tolist())])

        log("cargando tabla de hechos (fct_delitos)")
        columnas_fct = [
            "id_registro", "fecha_hecho", "anio", "mes", "cod_depto", "departamento",
            "cod_muni", "municipio", "codigo", "conducta", "cantidad",
        ]
        filas_fct = _filas(curado, columnas_fct)
        _insertar(cur, "service.fct_delitos", columnas_fct, filas_fct)
        conn.commit()

    metricas = json.load(open(os.path.join(config.EVIDENCIAS_DIR, "curated_metrics.json"), encoding="utf-8"))
    guardar_watermark(fecha_corte, int(metricas.get("filas_entrada") or total_registros))

    resumen = {
        "filas_fct": len(filas_fct),
        "filas_ind_casos_anio": por_anio.count(),
        "filas_ind_conducta_anio": conducta_anio.count(),
        "filas_ind_departamento_anio": depto_anio.count(),
        "filas_ind_municipio": municipio.count(),
        "filas_ind_mensual": mensual.count(),
        "total_casos": int(total_casos),
        "fecha_corte": str(fecha_corte),
    }
    log(f"zona de servicio lista: {resumen}")
    curado.unpersist()
    return resumen


def main() -> None:
    spark = (
        SparkSession.builder.appName("delitos-zona-servicio")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        resumen = cargar(spark)
        print(json.dumps(resumen, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
