# Checklist de entrega — Proyecto Pipeline de Datos G002

Entrega en Classroom: **martes 24 de noviembre de 2026, 11:59 p. m.**
Sustentación: martes 24 y jueves 26, 8:00 p. m. (10 min + 5 min de preguntas).

## Archivos que pide el enunciado

| Qué pide | Dónde está | Estado |
|---|---|---|
| Repositorio GitHub (código + README + diagrama) | <https://github.com/dev-plague/delitos-informaticos-colombia> | ✅ |
| `requirements.txt` | raíz del repositorio | ✅ |
| `docker-compose.yml` | raíz del repositorio | ✅ |
| Notebook o scripts fases 2 a 8, ejecutado, con explicaciones | `notebooks/pipeline_delitos.ipynb` (ejecutado) + `src/` | ✅ |
| Evidencias (Kafka, HDFS, MapReduce vs Spark, idempotencia, streaming) | `evidencias/` | ✅ |
| Muestra de la zona raw | `data/muestra_raw.jsonl` (1.000 líneas) | ✅ |
| Manifiesto(s) | `data/manifiestos/` | ✅ |
| Tablas de la zona de servicio | `data/servicio/*.csv` | ✅ |
| Tablero (HTML, enlace o app) | Streamlit en `http://localhost:8501` + capturas en `evidencias/` | ✅ |
| Presentación (máx. 8 diapositivas) | `docs/presentacion.html` (imprimir a PDF con **P**) | ✅ |

## Nombres para Classroom

Archivo único:

```
PP_G002_Ramirez.pdf
```

El proyecto es individual; el nombre del archivo corresponde a Alejandro Ramírez.

## Antes de subir

- [x] Nombre del integrante en `README.md` (Alejandro Ramírez).
- [ ] Verificar que el repositorio sea público o que el docente tenga acceso.
- [ ] Correr `bash run_pipeline.sh` en el equipo de la sustentación y dejar el tablero abierto.
- [ ] Tener a mano `evidencias/idempotencia_corridas.json` para la demo en vivo.
- [ ] Ensayar la demo: correr `bash scripts/demo_idempotencia.sh` dos veces y mostrar que
      los conteos no cambian.
- [ ] Exportar la presentación a PDF (abrir `docs/presentacion.html` y presionar `P`).

## Orden sugerido de la sustentación

1. Pregunta, fuente y arquitectura (Lambda).
2. Ingesta y Kafka (3 particiones, key = `id_registro`).
3. HDFS y MapReduce frente a Spark (0 diferencias).
4. Zona curada en Spark (Parquet, 89,7 % menos, partición por año).
5. Calidad e idempotencia (correr dos veces en vivo).
6. Streaming y reinicio sin duplicados.
7. Tablero y hallazgos.
