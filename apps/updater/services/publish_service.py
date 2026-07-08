import os
import sqlite3

from packages.logger.logger import get_logger
from packages.utils.files import safe_rename, remove_if_exists
from packages.database.sqlite import optimize_for_read
from packages.constants.constants import DB_FILENAME, DB_FILENAME_NEW, DB_FILENAME_OLD
from packages.exceptions import PublishError

logger = get_logger("updater.publish")


def validate_db(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM contribuyentes")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        conn.close()

        logger.info(f"Validación: {count} registros, {len(indexes)} índices")
        return count
    except Exception as e:
        raise PublishError(f"Error validando base de datos: {e}")


def publish(data_dir: str, db_path: str, new_db_path: str) -> str:
    count = validate_db(new_db_path)

    conn = sqlite3.connect(new_db_path)
    optimize_for_read(conn)
    conn.close()

    active_path = os.path.join(data_dir, DB_FILENAME)
    old_path = os.path.join(data_dir, DB_FILENAME_OLD)

    if os.path.exists(active_path):
        logger.info("Respaldando base actual → contribuyentes_old.db")
        safe_rename(active_path, old_path)

    logger.info("Publicando nueva base → contribuyentes.db")
    safe_rename(new_db_path, active_path)

    if os.path.exists(old_path):
        logger.info("Eliminando respaldo anterior")
        remove_if_exists(old_path)

    logger.info(f"Publicación exitosa: {count} registros")
    return active_path
