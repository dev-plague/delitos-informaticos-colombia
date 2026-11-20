"""FASE 3 - Productor de Kafka.

Publica cada línea del JSONL raw como un mensaje con key = id_registro.
La llave garantiza que todos los eventos de un mismo registro caigan en la
misma partición (orden por registro) y permite deduplicar aguas abajo.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

from . import config
from .common import leer_json, listar_jsonl, log


def _archivos_del_ultimo_manifiesto() -> list[str]:
    ruta = os.path.join(config.MANIFEST_DIR, "latest.json")
    if not os.path.exists(ruta):
        return []
    manifiesto = leer_json(ruta)
    return [a for a in manifiesto.get("archivos", []) if os.path.exists(a)]


def _crear_productor() -> KafkaProducer:
    opciones = dict(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=20,
        batch_size=65536,
        compression_type="gzip",
        max_in_flight_requests_per_connection=1,
    )
    try:
        # Evita duplicados por reintentos dentro de una misma sesión del productor
        return KafkaProducer(enable_idempotence=True, **opciones)
    except (TypeError, AssertionError):
        return KafkaProducer(**opciones)


def producir(
    topic: str = config.TOPIC_RAW,
    archivos: list[str] | None = None,
    desde: str | None = None,
    limite: int | None = None,
) -> dict:
    archivos = archivos or _archivos_del_ultimo_manifiesto() or listar_jsonl(config.RAW_DIR)
    if not archivos:
        log("no hay archivos JSONL para publicar; ejecute primero la ingesta")
        return {"topic": topic, "mensajes": 0, "archivos": 0}

    productor = _crear_productor()
    enviados = 0
    errores = 0
    por_particion: dict[int, int] = {}
    inicio = time.time()

    def _error(exc):
        nonlocal errores
        errores += 1
        log(f"error al publicar: {exc}")

    log(f"publicando en '{topic}' desde {len(archivos)} archivo(s)")
    for archivo in archivos:
        with open(archivo, encoding="utf-8") as fh:
            for linea in fh:
                linea = linea.strip()
                if not linea:
                    continue
                sobre = json.loads(linea)
                if desde and (sobre["payload"].get("fecha_hecho") or "")[:10] < desde:
                    continue
                futuro = productor.send(topic, key=sobre["id_registro"], value=linea)
                futuro.add_errback(_error)
                enviados += 1
                if limite and enviados >= limite:
                    break
                if enviados % 20000 == 0:
                    log(f"  {enviados} mensajes encolados...")
        if limite and enviados >= limite:
            break

    productor.flush(timeout=120)
    productor.close(timeout=30)
    duracion = round(time.time() - inicio, 2)
    resultado = {
        "topic": topic,
        "mensajes": enviados,
        "errores": errores,
        "archivos": len(archivos),
        "duracion_seg": duracion,
        "mensajes_por_seg": round(enviados / duracion, 1) if duracion else 0,
        "desde": desde,
    }
    log(f"FIN productor: {enviados} mensajes, {errores} errores, {duracion}s")
    return resultado


def _desde_reciente(archivos: list[str], dias: int = 90) -> str | None:
    """Fecha de corte (últimos `dias`) calculada sobre el máximo del dataset."""
    maximo = None
    for archivo in archivos:
        with open(archivo, encoding="utf-8") as fh:
            for linea in fh:
                fecha = (json.loads(linea)["payload"].get("fecha_hecho") or "")[:10]
                if fecha and (maximo is None or fecha > maximo):
                    maximo = fecha
    if not maximo:
        return None
    return (datetime.strptime(maximo, "%Y-%m-%d") - timedelta(days=dias)).strftime("%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publica la zona raw en Kafka")
    parser.add_argument("--topic", default=config.TOPIC_RAW)
    parser.add_argument("--todo", action="store_true", help="Reenvía toda la zona raw (replay)")
    parser.add_argument("--limite", type=int, help="Máximo de mensajes (pruebas)")
    parser.add_argument("--dias", type=int, help="Solo los últimos N días (capa rápida)")
    args = parser.parse_args()

    archivos = listar_jsonl(config.RAW_DIR) if args.todo else None
    desde = None
    if args.dias:
        base = archivos or _archivos_del_ultimo_manifiesto() or listar_jsonl(config.RAW_DIR)
        desde = _desde_reciente(base, args.dias)
        log(f"filtro de capa rápida: desde {desde}")
    producir(topic=args.topic, archivos=archivos, desde=desde, limite=args.limite)


if __name__ == "__main__":
    main()
