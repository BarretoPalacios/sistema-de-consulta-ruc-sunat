import os
import time

from apps.updater.services.download_service import check_remote, download_zip, save_metadata
from apps.updater.services.extract_service import extract_txt_from_zip
from apps.updater.services.index_service import create_indexes
from apps.updater.services.load_service import load_to_db
from apps.updater.services.publish_service import publish
from packages.constants.constants import DB_FILENAME, DB_FILENAME_NEW
from packages.logger.logger import get_logger
from packages.utils.files import check_disk_space, ensure_dir, remove_if_exists

logger = get_logger("updater")


def run_update(url: str, data_dir: str, temp_dir: str, force: bool = False):
    start = time.time()
    new_db_path = os.path.join(data_dir, DB_FILENAME_NEW)
    metadata_path = os.path.join(data_dir, "metadata.json")
    active_db = os.path.join(data_dir, DB_FILENAME)

    try:
        check_disk_space(data_dir)
        ensure_dir(temp_dir)
        ensure_dir(data_dir)

        if not force:
            needs_update, remote_info = check_remote(url, metadata_path)
            if not needs_update:
                logger.info("No hay cambios — omitiendo actualización")
                return {"success": True, "message": "Sin cambios", "updated": False}
        else:
            remote_info = {}

        zip_path = download_zip(url, temp_dir)

        txt_path = extract_txt_from_zip(zip_path, temp_dir)

        remove_if_exists(new_db_path)

        records, errors = load_to_db(txt_path, new_db_path)

        create_indexes(new_db_path)

        publish(data_dir, active_db, new_db_path)

        save_metadata(metadata_path, remote_info, records)

        elapsed = time.time() - start
        logger.info(f"Actualización completada en {elapsed:.1f}s: {records} registros, {errors} errores")

        return {
            "success": True,
            "records": records,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
            "updated": True,
        }

    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"Error en actualización: {e}", exc_info=True)
        if os.path.exists(new_db_path):
            remove_if_exists(new_db_path)
        return {
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
        }

    finally:
        remove_if_exists(temp_dir)
