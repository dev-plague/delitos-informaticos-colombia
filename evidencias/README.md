# Evidencias

Salidas reales del pipeline ejecutado en este equipo (20 de noviembre de 2026).

| Archivo | Qué prueba |
|---|---|
| `kafka_topico.txt` | Tópico `delitos-raw` con 3 particiones |
| `kafka_consumidor.txt` | Grupo `delitos-hdfs-writer`, offsets y lag 0 |
| `hdfs_estructura.txt` | Estructura `raw/fecha_ingesta=…`, `curated/anio=…`, 47 lotes |
| `hdfs_tamanos.txt` | Tamaño de la zona raw y de la curada en HDFS |
| `mapreduce_salida.txt` | Salida cruda del job de Hadoop Streaming |
| `mapreduce_vs_spark.md` | 48 claves comparadas, 0 diferencias |
| `curated_metrics.md` | Antes/después de la curaduría y reducción de tamaño |
| `calidad_reporte.md` | 7 validaciones y descartados |
| `idempotencia_corridas.json` | Dos corridas: mismos 232.939 registros y 236.883 casos |
| `streaming_reinicio.md` | Reinicio del flujo sin duplicar y comparación con batch |
| `streaming_vs_batch.json` | Agregado del flujo vs batch, 0 diferencias |
| `streaming_*.json` | Offsets y conteos de cada corrida del flujo |
| `tablero_*.jpg` | Capturas de las pestañas del tablero |
