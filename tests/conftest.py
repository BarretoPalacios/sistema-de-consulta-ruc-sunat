import os
import sqlite3
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def pytest_sessionstart():
    tmp_db = os.path.join(tempfile.gettempdir(), "test_contribuyentes.db")

    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contribuyentes (
            ruc TEXT PRIMARY KEY,
            nombre_razon_social TEXT,
            estado_contribuyente TEXT,
            condicion_domicilio TEXT,
            ubigeo TEXT,
            tipo_via TEXT,
            nombre_via TEXT,
            codigo_zona TEXT,
            tipo_zona TEXT,
            numero TEXT,
            interior TEXT,
            lote TEXT,
            departamento TEXT,
            manzana TEXT,
            kilometro TEXT
        )
    """)
    cursor.executemany(
        "INSERT OR REPLACE INTO contribuyentes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("10452159428", "JUAN PEREZ", "ACTIVO", "HABIDO", "150101", "AV", "LOS OLIVOS", "", "", "123", "", "", "", "", ""),
            ("20131312955", "EMPRESA SAC", "ACTIVO", "HABIDO", "150102", "JR", "LAS FLORES", "", "", "456", "", "", "", "", ""),
            ("20567890123", "COMERCIAL EIRL", "BAJA", "NO HABIDO", "070101", "CALLE", "LOS PINOS", "", "", "789", "", "", "", "", ""),
        ],
    )
    conn.commit()
    conn.close()

    from packages.config.settings import settings
    settings.DATABASE_URL = tmp_db

    from apps.api.app.core.config import api_settings
    os.environ["API_TOKEN"] = "test-token"
    api_settings.API_TOKEN = "test-token"


@pytest.fixture
def test_client():
    from apps.api.main import app
    return TestClient(app)
