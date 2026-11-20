# Datos de muestra

El dataset completo (113 MB en JSONL) no se versiona: se reproduce con
`docker compose exec -T spark python -m src.ingest`.

| Archivo | Contenido |
|---|---|
| `muestra_raw.jsonl` | 1.000 líneas de la zona raw con el sobre completo |
| `curated_sample.parquet` | 2.000 filas de la zona curada (Parquet, una sola pieza) |
| `raw/_manifests/` | Manifiestos de las corridas (URL, parámetros, conteos) |
| `manifiestos/*.json` | Copia versionada de los manifiestos |
| `raw/fecha_ingesta=…/` | Zona raw completa (ignorada por git) |
| `servicio/*.csv` | Tablas de la zona de servicio exportadas desde PostgreSQL |
