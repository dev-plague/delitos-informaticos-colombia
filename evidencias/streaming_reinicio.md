# Streaming: reinicio e idempotencia

## Corridas del flujo

| Corrida | Offset final | Filas en servicio | Casos |
|---|---|---:|---:|
| 2026-11-26T23:58:20 | `{'delitos-stream': {'2': 7154, '1': 7030, '0': 7187}}` | 669 | 21371 |
| 2026-11-26T23:58:54 | `{'delitos-stream': {'2': 7154, '1': 7030, '0': 7187}}` | 669 | 21371 |

La segunda corrida reutiliza el checkpoint de HDFS: continúa desde los offsets confirmados y el `upsert` por `(fecha, codigo)` deja el mismo número de filas y casos. No se reprocesa ni se duplica.

## Capa rápida vs capa batch (mismas fechas)

- Casos en streaming: **21,371**
- Casos en batch: **21,371**
- Filas por (fecha, artículo): stream=669, batch=669
- Diferencias: **0**

Las dos capas coinciden: el flujo diario reproduce exactamente el agregado batch de la ventana reciente (arquitectura Lambda).
