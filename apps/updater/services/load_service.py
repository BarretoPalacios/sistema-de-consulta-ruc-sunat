import sqlite3
import time
from typing import Tuple

from packages.logger.logger import get_logger
from packages.database.sqlite import optimize_for_bulk_insert, insert_batch
from packages.constants.constants import CREATE_TABLE_SQL
from packages.utils.encodings import detect_encoding
from apps.updater.services.parse_service import parse_line

logger = get_logger("updater.load")


def load_to_db(filepath: str, db_path: str) -> Tuple[int, int]:
    encoding = detect_encoding(filepath)
    logger.info(f"Codificación detectada: {encoding}")

    conn = sqlite3.connect(db_path)
    optimize_for_bulk_insert(conn)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS contribuyentes")
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()

    total_records = 0
    error_count = 0
    batch = []
    batch_size = 30000
    start_time = time.time()

    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        f.readline()

        for line in f:
            record = parse_line(line)
            if record:
                batch.append(record)
            else:
                error_count += 1

            if len(batch) >= batch_size:
                insert_batch(cursor, conn, batch)
                total_records += len(batch)
                batch = []

        if batch:
            insert_batch(cursor, conn, batch)
            total_records += len(batch)

    elapsed = time.time() - start_time
    logger.info(f"Carga completada: {total_records} registros en {elapsed:.1f}s ({error_count} errores)")
    conn.close()
    return total_records, error_count
