# Clúster HDFS mínimo (namenode + datanode) con Python 3 para Hadoop Streaming.
# Reutiliza los binarios Hadoop 3.3.6 de la imagen oficial sobre una base Debian
# (la imagen oficial es CentOS 7 y sus repositorios ya no están disponibles).
FROM eclipse-temurin:11-jre-jammy

ENV DEBIAN_FRONTEND=noninteractive \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/usr/local/bin:/usr/bin:/bin

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 curl procps ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=apache/hadoop:3.3.6 /opt/hadoop /opt/hadoop

COPY docker/hadoop-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
 && mkdir -p /data/name /data/data /data/tmp /opt/hadoop/logs \
 && printf 'export JAVA_HOME=/opt/java/openjdk\n' > /opt/hadoop/etc/hadoop/hadoop-env.sh

EXPOSE 9870 9000 9864 9866
ENTRYPOINT ["/entrypoint.sh"]
