#!/usr/bin/env bash
# FASE 7 - Demostración de reinicio del flujo sin duplicados.
#   1) limpia la tabla de la capa rápida y el checkpoint
#   2) corre el flujo 45 s
#   3) lo reinicia 25 s reutilizando el checkpoint
#   4) compara streaming vs batch y deja la evidencia
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[streaming] limpiando tabla de servicio"
docker compose exec -T postgres psql -U delitos -d delitos -c "TRUNCATE service.stream_diario" >/dev/null

echo "[streaming] corrida 1 (checkpoint nuevo)"
docker compose exec -T spark python -m src.streaming --duracion 45 --reset | tail -12

echo "[streaming] conteo tras corrida 1"
docker compose exec -T postgres psql -U delitos -d delitos -tAc \
  "SELECT count(*) || ' filas, ' || coalesce(sum(casos),0) || ' casos' FROM service.stream_diario"

echo "[streaming] corrida 2 (reinicio con checkpoint)"
docker compose exec -T spark python -m src.streaming --duracion 25 | tail -12

echo "[streaming] conteo tras reinicio"
docker compose exec -T postgres psql -U delitos -d delitos -tAc \
  "SELECT count(*) || ' filas, ' || coalesce(sum(casos),0) || ' casos' FROM service.stream_diario"

echo "[streaming] comparación streaming vs batch"
docker compose exec -T spark python -m src.compare_streaming | tail -5
