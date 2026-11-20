"""Registra el resultado de una corrida del pipeline (evidencia de idempotencia).

Se ejecuta al final de cada corrida: compara lo que entró por la API, lo que
quedó en la zona curada y lo que quedó en la zona de servicio.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

from . import config
from .common import leer_json, log, pg_connect

RUTA = os.path.join(config.EVIDENCIAS_DIR, "idempotencia_corridas.json")


def _json_si_existe(nombre: str) -> dict:
    ruta = os.path.join(config.EVIDENCIAS_DIR, nombre)
    return leer_json(ruta) if os.path.exists(ruta) else {}


def registrar(etiqueta: str) -> dict:
    manifiesto = leer_json(os.path.join(config.MANIFEST_DIR, "latest.json"))
    metricas = _json_si_existe("curated_metrics.json")
    calidad = _json_si_existe("calidad_reporte.json")

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(cantidad),0) FROM service.fct_delitos")
        filas_fct, casos = cur.fetchone()
        cur.execute("SELECT ultima_fecha, registros_raw FROM control.etl_control WHERE fuente=%s AND dataset=%s",
                    (config.FUENTE, config.DATASET_ID))
        marca = cur.fetchone()
        cur.execute("SELECT count(*) FROM service.stream_diario")
        filas_stream = cur.fetchone()[0]

    corrida = {
        "etiqueta": etiqueta,
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "run_id": manifiesto.get("run_id"),
        "registros_ingestados_api": manifiesto.get("registros"),
        "watermark_anterior": manifiesto.get("watermark_anterior"),
        "filas_raw_acumuladas": metricas.get("filas_entrada"),
        "duplicados_eliminados": metricas.get("duplicados_id_registro"),
        "filas_curadas": metricas.get("filas_salida"),
        "casos_totales": int(casos),
        "filas_zona_servicio": int(filas_fct),
        "filas_stream": int(filas_stream),
        "watermark_nueva": str(marca[0]) if marca else None,
        "estado_calidad": calidad.get("estado"),
    }

    historial = leer_json(RUTA) if os.path.exists(RUTA) else []
    historial.append(corrida)
    with open(RUTA, "w", encoding="utf-8") as fh:
        json.dump(historial, fh, ensure_ascii=False, indent=2)

    log(f"corrida registrada: {corrida}")
    return corrida


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--etiqueta", default="corrida")
    args = parser.parse_args()
    print(json.dumps(registrar(args.etiqueta), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
