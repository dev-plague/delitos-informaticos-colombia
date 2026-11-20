# Zona curada: antes y después

| Problema | Filas afectadas | Decisión |
|---|---:|---|
| Nulos en columnas clave | 0 | Se descartan: no se puede reconstruir el hecho |
| Duplicados por `id_registro` | 130 | Se conserva la primera aparición |
| `cantidad` fuera de rango | 0 | Se descartan valores <= 0 o > 1000 |
| Fechas fuera de rango | 0 | Se descartan fechas < 2000 o futuras |
| Códigos DANE inválidos | 0 | Se exige longitud 2 (depto) y 5 (municipio) |

**Filas con contenido idéntico conservadas:** 120,245. No son duplicados del pipeline: el dataset publica un registro por caso y dos casos pueden coincidir en fecha, municipio, conducta y cantidad. El `id_registro` (`:id` de Socrata) los distingue.

**Filas de entrada:** 233,069 → **filas curadas:** 232,939 (descartadas 130).

## Tamaño JSONL vs Parquet

| Zona | Bytes |
|---|---:|
| Raw HDFS (JSONL) | 118,963,681 |
| Curado HDFS (Parquet) | 12,290,007 |
| Reducción | 89.67% |

**Partición elegida:** `anio`. El tablero y las validaciones filtran por año, la cardinalidad es baja (4 carpetas) y evita el problema de miles de carpetas que tendría particionar por municipio (1.018 valores).
