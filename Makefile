.PHONY: up down logs pipeline idempotencia streaming tablero notebook clean

up:            ## Levanta la plataforma
	docker compose up -d --wait

down:          ## Detiene la plataforma (conserva los volúmenes)
	docker compose down

logs:          ## Logs de todos los servicios
	docker compose logs -f --tail=50

pipeline:      ## Pipeline completo
	bash run_pipeline.sh

idempotencia:  ## Segunda corrida: demuestra que no duplica
	bash scripts/demo_idempotencia.sh

streaming:     ## Demostración de reinicio del flujo
	bash scripts/run_streaming_demo.sh

tablero:       ## Abre el tablero
	xdg-open http://localhost:8501 || true

notebook:      ## Ejecuta el notebook del proyecto dentro del contenedor
	docker compose exec -T spark jupyter nbconvert --to notebook --execute --inplace \
		--ExecutePreprocessor.timeout=600 notebooks/pipeline_delitos.ipynb

clean:         ## Borra contenedores y volúmenes (¡pierde HDFS y PostgreSQL!)
	docker compose down -v
