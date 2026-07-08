import sqlite3

from packages.logger.logger import get_logger
from packages.constants.constants import INDEXES

logger = get_logger("updater.index")


def create_indexes(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = OFF")
    cursor = conn.cursor()

    for name, sql in INDEXES:
        try:
            cursor.execute(sql)
            logger.info(f"Índice creado: {name}")
        except Exception as e:
            logger.warning(f"Error creando índice {name}: {e}")

    conn.commit()
    conn.close()
