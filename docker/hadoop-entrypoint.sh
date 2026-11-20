#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  namenode)
    if [ ! -f /data/name/current/VERSION ]; then
      echo "[entrypoint] Formateando namenode por primera vez..."
      hdfs namenode -format -force -nonInteractive
    fi
    echo "[entrypoint] Iniciando namenode"
    exec hdfs namenode
    ;;
  datanode)
    echo "[entrypoint] Iniciando datanode"
    exec hdfs datanode
    ;;
  *)
    exec "$@"
    ;;
esac
