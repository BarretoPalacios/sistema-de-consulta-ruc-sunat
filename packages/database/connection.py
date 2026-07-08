import sqlite3


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    from packages.config.settings import settings
    path = db_path or settings.DATABASE_URL
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def get_cursor(conn: sqlite3.Connection) -> sqlite3.Cursor:
    return conn.cursor()
