import os
import json
import requests
from typing import Optional

from packages.logger.logger import get_logger
from packages.utils.files import ensure_dir
from packages.exceptions import DownloadError

logger = get_logger("updater.download")


def check_remote(url: str, metadata_path: str) -> tuple[bool, dict]:
    try:
        resp = requests.head(url, timeout=30)
        resp.raise_for_status()
        remote = {
            "etag": resp.headers.get("ETag", ""),
            "last_modified": resp.headers.get("Last-Modified", ""),
            "content_length": resp.headers.get("Content-Length", "0"),
        }

        if os.path.exists(metadata_path):
            with open(metadata_path, "r") as f:
                local = json.load(f)
            if (remote["etag"] and remote["etag"] == local.get("etag")) or \
               (remote["last_modified"] and remote["last_modified"] == local.get("last_modified")):
                logger.info("Sin cambios detectados — skipping descarga")
                return False, remote

        return True, remote
    except requests.RequestException as e:
        logger.warning(f"Error en HEAD request: {e}")
        return True, {}


def download_zip(url: str, temp_dir: str) -> str:
    try:
        ensure_dir(temp_dir)
        zip_path = os.path.join(temp_dir, "padron_reducido.zip")
        logger.info(f"Descargando {url} ...")

        response = requests.get(url, timeout=600, stream=True)
        response.raise_for_status()

        total = 0
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                total += len(chunk)

        logger.info(f"ZIP descargado: {total / 1024 / 1024:.1f} MB")
        return zip_path
    except requests.RequestException as e:
        raise DownloadError(f"Error al descargar ZIP: {e}")


def save_metadata(metadata_path: str, remote: dict, records: int = 0):
    ensure_dir(os.path.dirname(metadata_path))
    data = {
        "etag": remote.get("etag", ""),
        "last_modified": remote.get("last_modified", ""),
        "content_length": remote.get("content_length", "0"),
        "records": records,
    }
    with open(metadata_path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Metadatos guardados en {metadata_path}")
