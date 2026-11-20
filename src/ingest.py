"""FASE 2 - Ingesta a la zona raw.

Descarga la API de Socrata con paginación y reintentos, envuelve cada registro
en un sobre JSON (fuente, dataset, id_registro, fecha_ingesta, payload) y
escribe JSON Lines particionado por fecha_ingesta=AAAA-MM-DD.
Cada corrida deja un manifiesto con URL, parámetros y conteo.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import timedelta

import requests

from . import config, hdfs_client
from .common import (
    ahora_utc,
    asegurar_dir,
    envolver,
    escribir_json,
    fecha_hoy,
    iso_utc,
    leer_watermark,
    listar_jsonl,
    log,
)


def _sesion() -> requests.Session:
    sesion = requests.Session()
    sesion.headers.update({"User-Agent": "pipeline-delitos-informaticos/1.0 (curso G002)"})
    return sesion


def _pedir_pagina(sesion: requests.Session, params: dict) -> list[dict]:
    """GET con reintentos exponenciales ante 429/5xx/timeouts."""
    ultimo_error: Exception | None = None
    for intento in range(1, config.MAX_REINTENTOS + 1):
        try:
            resp = sesion.get(config.API_URL, params=params, timeout=config.TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            ultimo_error = exc
            espera = min(2 ** intento, 30)
            log(f"  intento {intento}/{config.MAX_REINTENTOS} falló ({exc}); reintento en {espera}s")
            time.sleep(espera)
    raise RuntimeError(f"la API falló tras {config.MAX_REINTENTOS} intentos: {ultimo_error}")


def _condicion_fecha(watermark) -> str:
    if watermark is not None:
        # Se reprocesa el último día para capturar correcciones del origen;
        # los duplicados se eliminan por id_registro en la zona curada.
        return f"fecha_hecho >= '{watermark:%Y-%m-%dT%H:%M:%S}'"
    return f"fecha_hecho >= '{config.DESDE}'"


def ingestar(desde_override: str | None = None, max_paginas: int | None = None) -> dict:
    run_id = f"{ahora_utc():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    fecha_ingesta = iso_utc()
    particion = f"fecha_ingesta={fecha_hoy()}"
    dir_local = asegurar_dir(os.path.join(config.RAW_DIR, particion))
    asegurar_dir(config.MANIFEST_DIR)
    asegurar_dir(config.EVIDENCIAS_DIR)

    watermark = None if desde_override else leer_watermark()
    where = _condicion_fecha(watermark) if not desde_override else f"fecha_hecho >= '{desde_override}'"
    if config.HASTA:
        where += f" AND fecha_hecho <= '{config.HASTA}'"

    log(f"run_id={run_id}")
    log(f"watermark={watermark}  filtro: {where}")

    sesion = _sesion()
    offset = 0
    pagina = 0
    registros = 0
    bytes_escritos = 0
    archivos: list[str] = []
    primera_fecha = None
    ultima_fecha = None
    inicio = time.time()

    while True:
        parametros = {
            "$select": ":id,fecha_hecho,cod_depto,departamento,cod_muni,municipio,descripcion_conducta,cantidad",
            "$limit": config.PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
            "$where": where,
        }
        log(f"página {pagina}: offset={offset} limit={config.PAGE_SIZE}")
        filas = _pedir_pagina(sesion, parametros)
        if not filas:
            break

        nombre = f"part-{run_id}-{pagina:05d}.jsonl"
        ruta = os.path.join(dir_local, nombre)
        with open(ruta, "w", encoding="utf-8") as fh:
            for fila in filas:
                sobre = envolver(fila, fecha_ingesta)
                fh.write(json.dumps(sobre, ensure_ascii=False) + "\n")
                fecha = (fila.get("fecha_hecho") or "")[:10]
                if fecha:
                    primera_fecha = min(primera_fecha, fecha) if primera_fecha else fecha
                    ultima_fecha = max(ultima_fecha, fecha) if ultima_fecha else fecha
        tam = os.path.getsize(ruta)
        bytes_escritos += tam
        archivos.append(ruta)
        registros += len(filas)
        log(f"  {len(filas)} registros -> {nombre} ({tam/1024:.1f} KB)")

        pagina += 1
        offset += len(filas)
        if len(filas) < config.PAGE_SIZE:
            break
        if max_paginas and pagina >= max_paginas:
            log("límite de páginas alcanzado")
            break

    duracion = round(time.time() - inicio, 2)
    manifiesto = {
        "run_id": run_id,
        "fuente": config.FUENTE,
        "entidad": config.ENTIDAD,
        "dataset": config.DATASET_ID,
        "dataset_nombre": config.DATASET_TITULO,
        "url": config.API_URL,
        "parametros": {
            "$select": ":id,fecha_hecho,cod_depto,departamento,cod_muni,municipio,descripcion_conducta,cantidad",
            "$limit": config.PAGE_SIZE,
            "$order": ":id",
            "$where": where,
        },
        "ventana": {"desde": config.DESDE, "hasta": config.HASTA or "sin límite"},
        "watermark_anterior": watermark.isoformat() if watermark else None,
        "fecha_ingesta": fecha_ingesta,
        "particion": particion,
        "paginas": pagina,
        "registros": registros,
        "bytes": bytes_escritos,
        "archivos": archivos,
        "rango_fechas": {"min": primera_fecha, "max": ultima_fecha},
        "duracion_seg": duracion,
        "estado": "ok",
    }

    ruta_manifiesto = os.path.join(config.MANIFEST_DIR, f"manifest_{run_id}.json")
    escribir_json(ruta_manifiesto, manifiesto)
    escribir_json(os.path.join(config.MANIFEST_DIR, "latest.json"), manifiesto)

    # El manifiesto también queda en HDFS (trazabilidad de la zona raw)
    try:
        hdfs_client.mkdirs(f"{config.HDFS_RAW}/_manifests")
        hdfs_client.create(
            f"{config.HDFS_RAW}/_manifests/manifest_{run_id}.json",
            json.dumps(manifiesto, ensure_ascii=False, indent=2).encode(),
        )
    except Exception as exc:
        log(f"aviso: no se pudo copiar el manifiesto a HDFS ({exc})")

    # Muestra pequeña para el repositorio (el dataset completo no se versiona)
    try:
        _guardar_muestra()
    except Exception as exc:
        log(f"aviso: no se pudo generar la muestra ({exc})")

    log(f"FIN ingesta: {registros} registros en {pagina} páginas, {bytes_escritos/1024/1024:.1f} MB, {duracion}s")
    log(f"manifiesto: {ruta_manifiesto}")
    return manifiesto


def _guardar_muestra(n: int = 1000) -> None:
    ruta = os.path.join(config.DATA_DIR, "muestra_raw.jsonl")
    asegurar_dir(config.DATA_DIR)
    escritas = 0
    with open(ruta, "w", encoding="utf-8") as salida:
        for archivo in listar_jsonl(config.RAW_DIR):
            if escritas >= n:
                break
            with open(archivo, encoding="utf-8") as entrada:
                for linea in entrada:
                    if escritas >= n:
                        break
                    salida.write(linea)
                    escritas += 1
    log(f"muestra raw: {escritas} líneas en {ruta}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingesta de la API a la zona raw (JSONL)")
    parser.add_argument("--desde", help="Ignora la marca de agua y usa esta fecha ISO")
    parser.add_argument("--max-paginas", type=int, default=None, help="Para pruebas rápidas")
    args = parser.parse_args()
    manifiesto = ingestar(desde_override=args.desde, max_paginas=args.max_paginas)
    if manifiesto["registros"] == 0:
        log("no hay registros nuevos sobre la marca de agua (idempotencia)")


if __name__ == "__main__":
    main()
