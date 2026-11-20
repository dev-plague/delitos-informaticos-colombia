# Imagen de cómputo: PySpark + JupyterLab + clientes de Kafka/HDFS/PostgreSQL
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless procps curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir \
      pyspark==3.5.1 \
      pandas==2.2.3 \
      pyarrow==17.0.0 \
      requests==2.32.3 \
      kafka-python-ng==2.2.3 \
      psycopg2-binary==2.9.10 \
      jupyterlab==4.2.5

# Conectores de Kafka para Structured Streaming (Spark 3.5.1 / Scala 2.12)
RUN mkdir -p /opt/spark-jars \
 && cd /opt/spark-jars \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && ls -lh /opt/spark-jars

COPY docker/spark-defaults.conf /usr/local/lib/python3.11/site-packages/pyspark/conf/spark-defaults.conf

RUN useradd -m -u 1000 -s /bin/bash spark
ENV HOME=/home/spark

WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
