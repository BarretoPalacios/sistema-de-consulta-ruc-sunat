import sqlite3


def optimize_for_bulk_insert(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -2000")
    conn.execute("PRAGMA temp_store = MEMORY")


def optimize_for_read(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -8000")


def insert_batch(cursor: sqlite3.Cursor, conn: sqlite3.Connection, batch: list[list], columns: int = 15):
    placeholders = ",".join(["?"] * columns)
    sql = f"INSERT OR REPLACE INTO contribuyentes VALUES ({placeholders})"
    try:
        cursor.executemany(sql, batch)
        conn.commit()
    except Exception:
        for record in batch:
            try:
                cursor.execute(sql, record)
            except Exception:
                pass
        conn.commit()
