"""Cliente mínimo de HDFS sobre la API WebHDFS (sin binarios Hadoop).

Se usa para que la ingesta y el consumidor de Kafka escriban en HDFS desde
Python puro; Spark y MapReduce sí hablan HDFS nativo con hdfs://.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

import requests

from . import config


class HDFSError(RuntimeError):
    pass


def _ruta(path: str) -> str:
    """Convierte hdfs://namenode:9000/data/x en /data/x para WebHDFS."""
    if path.startswith("hdfs://"):
        sin_esquema = path.split("://", 1)[1]
        return "/" + sin_esquema.split("/", 1)[1] if "/" in sin_esquema else "/"
    return path if path.startswith("/") else "/" + path


def _url(path: str) -> str:
    return config.WEBHDFS_URL.rstrip("/") + _ruta(path)


def _con_reintentos(metodo: str, url: str, **kwargs) -> requests.Response:
    ultimo: Exception | None = None
    for intento in range(1, 4):
        try:
            resp = requests.request(metodo, url, timeout=120, **kwargs)
            if resp.status_code in (500, 502, 503, 504):
                raise HDFSError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return resp
        except Exception as exc:  # red, timeouts, 5xx
            ultimo = exc
            time.sleep(2 ** intento)
    raise HDFSError(f"WebHDFS no respondió ({metodo} {url}): {ultimo}")


def exists(path: str) -> bool:
    resp = _con_reintentos("GET", _url(path) + "?op=GETFILESTATUS")
    if resp.status_code == 404:
        return False
    if resp.status_code != 200:
        raise HDFSError(f"GETFILESTATUS {path}: {resp.status_code} {resp.text[:200]}")
    return True


def mkdirs(path: str) -> None:
    resp = _con_reintentos("PUT", _url(path) + "?op=MKDIRS&permission=777")
    if resp.status_code != 200:
        raise HDFSError(f"MKDIRS {path}: {resp.status_code} {resp.text[:200]}")


def create(path: str, data: bytes, overwrite: bool = True) -> int:
    """Escribe un archivo (un solo PUT con redirección al datanode)."""
    params = f"?op=CREATE&overwrite={'true' if overwrite else 'false'}&permission=666"
    resp = _con_reintentos(
        "PUT",
        _url(path) + params,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        allow_redirects=True,
    )
    if resp.status_code not in (200, 201):
        raise HDFSError(f"CREATE {path}: {resp.status_code} {resp.text[:200]}")
    return len(data)


def write_lines(path: str, lineas: List[str]) -> int:
    """Escribe una lista de líneas (una por mensaje) como un solo archivo."""
    contenido = ("\n".join(lineas) + "\n").encode("utf-8")
    return create(path, contenido)


def read_text(path: str) -> str:
    resp = _con_reintentos("GET", _url(path) + "?op=OPEN", allow_redirects=True)
    if resp.status_code != 200:
        raise HDFSError(f"OPEN {path}: {resp.status_code} {resp.text[:200]}")
    return resp.text


def listdir(path: str) -> List[Dict[str, Any]]:
    resp = _con_reintentos("GET", _url(path) + "?op=LISTSTATUS")
    if resp.status_code == 404:
        return []
    if resp.status_code != 200:
        raise HDFSError(f"LISTSTATUS {path}: {resp.status_code} {resp.text[:200]}")
    return resp.json()["FileStatuses"]["FileStatus"]


def content_summary(path: str) -> Dict[str, int]:
    resp = _con_reintentos("GET", _url(path) + "?op=GETCONTENTSUMMARY")
    if resp.status_code != 200:
        raise HDFSError(f"GETCONTENTSUMMARY {path}: {resp.status_code} {resp.text[:200]}")
    resumen = resp.json()["ContentSummary"]
    return {
        "length": int(resumen.get("length", 0)),
        "spaceConsumed": int(resumen.get("spaceConsumed", 0)),
        "fileCount": int(resumen.get("fileCount", 0)),
        "directoryCount": int(resumen.get("directoryCount", 0)),
    }


def du(path: str) -> int:
    """Bytes lógicos de un directorio (0 si no existe)."""
    if not exists(path):
        return 0
    return content_summary(path)["length"]


def delete_recursive(path: str) -> None:
    resp = _con_reintentos("DELETE", _url(path) + "?op=DELETE&recursive=true")
    if resp.status_code not in (200, 404):
        raise HDFSError(f"DELETE {path}: {resp.status_code} {resp.text[:200]}")

