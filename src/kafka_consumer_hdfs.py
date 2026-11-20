"""FASE 3 y 4 - Consumidor de Kafka que escribe lotes en HDFS.

Un archivo por lote (no uno por mensaje) en:
  /data/delitos_informaticos/raw/fecha_ingesta=AAAA-MM-DD/lote-<ts>-<n>.jsonl

Los offsets se confirman (commit) sólo después de escribir el lote en HDFS:
garantía at-least-once. Los duplicados que pueda dejar un reintento se
eliminan después por id_registro en la zona curada (idempotencia).
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

from kafka import KafkaConsumer, TopicPartition

from . import config, hdfs_client
from .common import fecha_hoy, log


def consumir(
    topic: str = config.TOPIC_RAW,
    grupo: str = config.CONSUMER_GROUP,
    tamano_lote: int = 5000,
    segundos_lote: int = 10,
    timeout_inactividad: int = 20,
    max_mensajes: int | None = None,
) -> dict:
    consumidor = KafkaConsumer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        group_id=grupo,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=5000,
        session_timeout_ms=30000,
        request_timeout_ms=40000,
    )
    consumidor.subscribe([topic])
    log(f"consumidor grupo='{grupo}' tópico='{topic}'")

    particion_dir = f"{config.HDFS_RAW}/fecha_ingesta={fecha_hoy()}"
    hdfs_client.mkdirs(particion_dir)

    buffer: list[str] = []
    total = 0
    archivos: list[str] = []
    inicio = time.time()
    ultimo_mensaje = time.time()
    ultimo_flush = time.time()
    secuencia = 0
    particiones_vistas: set[int] = set()
    offsets_finales: dict[str, int] = {}

    def _flush() -> None:
        nonlocal buffer, ultimo_flush, secuencia
        if not buffer:
            return
        sello = datetime.now().strftime("%Y%m%dT%H%M%S")
        ruta = f"{particion_dir}/lote-{sello}-{secuencia:05d}.jsonl"
        hdfs_client.write_lines(ruta, buffer)
        archivos.append(ruta)
        log(f"  lote {secuencia:05d}: {len(buffer)} mensajes -> {ruta}")
        buffer = []
        secuencia += 1
        ultimo_flush = time.time()
        # Confirmar después de escribir en HDFS (at-least-once)
        consumidor.commit()

    try:
        while True:
            lotes = consumidor.poll(timeout_ms=1000, max_records=5000)
            if lotes:
                for tp, mensajes in lotes.items():
                    particiones_vistas.add(tp.partition)
                    for msg in mensajes:
                        buffer.append(msg.value.decode("utf-8"))
                        total += 1
                        if len(buffer) >= tamano_lote:
                            _flush()
                        if max_mensajes and total >= max_mensajes:
                            break
                ultimo_mensaje = time.time()
                for tp in lotes:
                    offsets_finales[str(tp.partition)] = consumidor.position(tp)

            if buffer and time.time() - ultimo_flush >= segundos_lote:
                _flush()

            if max_mensajes and total >= max_mensajes:
                break
            # Si no hay mensajes nuevos (lag = 0) se cierra el lote y se sale
            if not lotes and time.time() - ultimo_mensaje > timeout_inactividad:
                log(f"sin mensajes nuevos durante {timeout_inactividad}s; se cierra el consumidor")
                break
    finally:
        _flush()
        try:
            asignadas = consumidor.assignment()
            if asignadas:
                finales = consumidor.end_offsets(list(asignadas))
                for tp, offset in finales.items():
                    offsets_finales[str(tp.partition)] = max(offsets_finales.get(str(tp.partition), 0), offset)
        except Exception:
            pass
        consumidor.close()

    resultado = {
        "topic": topic,
        "grupo": grupo,
        "mensajes": total,
        "lotes": len(archivos),
        "archivos": archivos,
        "particiones": sorted(particiones_vistas),
        "offsets_finales": offsets_finales,
        "duracion_seg": round(time.time() - inicio, 2),
    }
    log(f"FIN consumidor: {total} mensajes en {len(archivos)} lotes HDFS")
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka y escribe lotes en HDFS")
    parser.add_argument("--topic", default=config.TOPIC_RAW)
    parser.add_argument("--grupo", default=config.CONSUMER_GROUP)
    parser.add_argument("--lote", type=int, default=5000, help="Mensajes por archivo de lote")
    parser.add_argument("--max", type=int, help="Máximo de mensajes (pruebas)")
    parser.add_argument("--inactividad", type=int, default=20)
    args = parser.parse_args()
    consumir(
        topic=args.topic,
        grupo=args.grupo,
        tamano_lote=args.lote,
        max_mensajes=args.max,
        timeout_inactividad=args.inactividad,
    )


if __name__ == "__main__":
    main()
