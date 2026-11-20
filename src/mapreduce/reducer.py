#!/usr/bin/env python3
"""Reducer de Hadoop Streaming: suma los valores de cada clave."""
import sys


def main() -> None:
    clave_actual = None
    suma = 0
    for linea in sys.stdin:
        linea = linea.rstrip("\n")
        if not linea:
            continue
        try:
            clave, valor = linea.split("\t", 1)
            valor = int(float(valor))
        except ValueError:
            continue
        if clave != clave_actual:
            if clave_actual is not None:
                print(f"{clave_actual}\t{suma}")
            clave_actual = clave
            suma = 0
        suma += valor
    if clave_actual is not None:
        print(f"{clave_actual}\t{suma}")


if __name__ == "__main__":
    main()
