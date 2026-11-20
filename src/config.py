"""Configuración central del pipeline.

Todos los valores se pueden sobreescribir con variables de entorno para que
el mismo código corra en Docker (valores por defecto) o en local.
"""
import os

# --------------------------------------------------------------------------
# Fuente de datos: DELITOS INFORMÁTICOS - datos.gov.co (Socrata)
# https://www.datos.gov.co/Seguridad-y-Defensa/DELITOS-INFORM-TICOS/4v6r-wu98
# --------------------------------------------------------------------------
DATASET_ID = os.getenv("DELITOS_DATASET_ID", "4v6r-wu98")
DATASET = "delitos_informaticos"
DATASET_TITULO = "DELITOS INFORMÁTICOS"
FUENTE = "datos.gov.co"
ENTIDAD = "Ministerio de Defensa Nacional - MinDefensa"

API_URL = f"https://www.datos.gov.co/resource/{DATASET_ID}.json"
META_URL = f"https://www.datos.gov.co/api/views/{DATASET_ID}.json"

# Ventana de análisis: últimos 3 años del dataset (máximo observado 2026-10-31)
DESDE = os.getenv("DELITOS_DESDE", "2023-11-01T00:00:00")
HASTA = os.getenv("DELITOS_HASTA", "")          # vacío = sin límite superior
PAGE_SIZE = int(os.getenv("DELITOS_PAGE_SIZE", "50000"))
MAX_REINTENTOS = int(os.getenv("DELITOS_RETRIES", "5"))
TIMEOUT = int(os.getenv("DELITOS_TIMEOUT", "90"))

# --------------------------------------------------------------------------
# Rutas locales (el repositorio se monta en /workspace dentro de los contenedores)
# --------------------------------------------------------------------------
WORKSPACE = os.getenv("WORKSPACE", "/workspace")
DATA_DIR = os.path.join(WORKSPACE, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
MANIFEST_DIR = os.path.join(RAW_DIR, "_manifests")
EVIDENCIAS_DIR = os.path.join(WORKSPACE, "evidencias")

# --------------------------------------------------------------------------
# HDFS (WebHDFS para escrituras desde Python, hdfs:// para Spark/MapReduce)
# --------------------------------------------------------------------------
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_BASE = f"{HDFS_URI}/data/{DATASET}"
HDFS_RAW = f"{HDFS_BASE}/raw"
HDFS_CURATED = f"{HDFS_BASE}/curated"
HDFS_MAPREDUCE = f"{HDFS_BASE}/mapreduce"
HDFS_STREAMING = f"{HDFS_BASE}/streaming"
HDFS_CHECKPOINTS = f"{HDFS_BASE}/checkpoints"

# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("DELITOS_TOPIC_RAW", "delitos-raw")
TOPIC_STREAM = os.getenv("DELITOS_TOPIC_STREAM", "delitos-stream")
TOPIC_PARTITIONS = 3
CONSUMER_GROUP = os.getenv("DELITOS_CONSUMER_GROUP", "delitos-hdfs-writer")

# --------------------------------------------------------------------------
# PostgreSQL (zona de servicio)
# --------------------------------------------------------------------------
PG = {
    "host": os.getenv("PGHOST", "postgres"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "delitos"),
    "user": os.getenv("PGUSER", "delitos"),
    "password": os.getenv("PGPASSWORD", "delitos"),
}
