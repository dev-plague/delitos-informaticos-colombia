#!/usr/bin/env python3
"""Mapper de Hadoop Streaming.

Entrada: una línea JSONL con el sobre del pipeline.
Salida: pares clave\tvalor para tres agregaciones con la MISMA lógica que
        el job de Spark de comparación.
"""
import json
import sys


def emitir(clave: str, valor: int) -> None:
    print(f"{clave}\t{valor}")


def main() -> None:
    for linea in sys.stdin:
        linea = linea.strip()
        if not linea:
            continue
        try:
            sobre = json.loads(linea)
            registro = sobre.get("payload", sobre)
            cantidad = int(float(registro.get("cantidad") or 0))
            if cantidad <= 0:
                continue
            conducta = " ".join(str(registro.get("descripcion_conducta") or "SIN_DATO").split())
            depto = " ".join(str(registro.get("departamento") or "SIN_DATO").split())
            anio = str(registro.get("fecha_hecho") or "")[:4] or "SIN_FECHA"
        except Exception:
            emitir("ERROR\t0", 1)
            continue

        emitir(f"CONDUCTA|{conducta}", cantidad)
        emitir(f"DEPARTAMENTO|{depto}", cantidad)
        emitir(f"ANIO|{anio}", cantidad)
        emitir("TOTAL|filas", 1)


if __name__ == "__main__":
    main()
