"""Utilidades compartidas: sobres JSON, id_registro, JSONL, PostgreSQL y logs."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator

from . import config


# --------------------------------------------------------------------------
# Logs
# --------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime | None = None) -> str:
    return (dt or ahora_utc()).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def fecha_hoy() -> str:
    return ahora_utc().strftime("%Y-%m-%d")


# --------------------------------------------------------------------------
# id_registro
# --------------------------------------------------------------------------
def _norm_texto(valor: Any) -> str:
    return " ".join(str(valor or "").strip().upper().split())


def construir_id_registro(payload: Dict[str, Any]) -> str:
    """Identificador de un registro de la fuente.

    El dataset no publica una llave de negocio: es una tabla de casos con
    (fecha, municipio, conducta, cantidad) y varias filas pueden ser idénticas
    sin ser el mismo hecho (dos víctimas el mismo día en el mismo municipio).
    Por eso se usa el identificador de fila de Socrata (`:id`), que es único
    y estable para el dataset. Si algún día no viniera, se cae a un hash de
    todos los campos para no romper el pipeline.
    """
    id_fuente = payload.get(":id") or payload.get("id_fuente")
    if id_fuente:
        return str(id_fuente)
    return hash_contenido(payload)


def hash_contenido(payload: Dict[str, Any]) -> str:
    """Hash de todos los campos del hecho (detecta filas idénticas)."""
    cantidad = payload.get("cantidad")
    try:
        cantidad = int(float(cantidad))
    except (TypeError, ValueError):
        cantidad = None
    clave = "|".join(
        [
            str(payload.get("fecha_hecho") or "").strip()[:10],
            _norm_texto(payload.get("cod_depto")),
            _norm_texto(payload.get("cod_muni")),
            _norm_texto(payload.get("descripcion_conducta")),
            "" if cantidad is None else str(cantidad),
        ]
    )
    return hashlib.sha1(clave.encode("utf-8")).hexdigest()


def envolver(payload: Dict[str, Any], fecha_ingesta: str) -> Dict[str, Any]:
    """Sobre JSON que viaja por todo el pipeline (raw -> Kafka -> HDFS)."""
    return {
        "fuente": config.FUENTE,
        "dataset": config.DATASET_ID,
        "id_registro": construir_id_registro(payload),
        "id_fuente": payload.get(":id"),
        "hash_contenido": hash_contenido(payload),
        "fecha_ingesta": fecha_ingesta,
        "payload": payload,
    }


# --------------------------------------------------------------------------
# Archivos
# --------------------------------------------------------------------------
def asegurar_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def escribir_json(path: str, obj: Any) -> None:
    asegurar_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
    log(f"escrito {path}")


def leer_json(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def leer_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if linea:
                yield json.loads(linea)


def listar_jsonl(directorio: str) -> list[str]:
    """Todos los .jsonl de la zona raw local, en orden estable."""
    archivos: list[str] = []
    for raiz, _dirs, nombres in os.walk(directorio):
        for nombre in sorted(nombres):
            if nombre.endswith(".jsonl"):
                archivos.append(os.path.join(raiz, nombre))
    return sorted(archivos)


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------
def pg_connect():
    import psycopg2

    return psycopg2.connect(**config.PG)


def leer_watermark(fuente: str = config.FUENTE, dataset: str = config.DATASET_ID):
    """Última fecha_hecho cargada en la zona de servicio (marca de agua)."""
    try:
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT ultima_fecha FROM control.etl_control WHERE fuente=%s AND dataset=%s",
                (fuente, dataset),
            )
            fila = cur.fetchone()
            return fila[0] if fila else None
    except Exception as exc:  # la zona de servicio puede no existir aún
        log(f"sin marca de agua disponible ({exc.__class__.__name__}); se usa la ventana inicial")
        return None


def guardar_watermark(ultima_fecha, registros: int) -> None:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO control.etl_control
                (fuente, dataset, ultima_fecha, ultima_corrida, registros_raw)
            VALUES (%s, %s, %s, now(), %s)
            ON CONFLICT (fuente, dataset) DO UPDATE SET
                ultima_fecha   = GREATEST(etl_control.ultima_fecha, EXCLUDED.ultima_fecha),
                ultima_corrida = now(),
                registros_raw  = EXCLUDED.registros_raw
            """,
            (config.FUENTE, config.DATASET_ID, ultima_fecha, registros),
        )
        conn.commit()


def error(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
