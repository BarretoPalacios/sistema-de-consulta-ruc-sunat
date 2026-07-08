from packages.database.connection import get_connection
from packages.config.settings import settings
from apps.api.app.core.config import api_settings
from apps.api.app.core.security import verify_token


def get_db():
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
