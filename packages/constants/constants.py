TABLE_NAME = "contribuyentes"
DB_FILENAME = "contribuyentes.db"
DB_FILENAME_NEW = "contribuyentes_new.db"
DB_FILENAME_OLD = "contribuyentes_old.db"
METADATA_FILE = "data/metadata.json"
PADRON_URL = "http://www2.sunat.gob.pe/padron_reducido_ruc.zip"

COLUMNS = [
    "ruc", "nombre_razon_social", "estado_contribuyente", "condicion_domicilio",
    "ubigeo", "tipo_via", "nombre_via", "codigo_zona", "tipo_zona",
    "numero", "interior", "lote", "departamento", "manzana", "kilometro"
]

CREATE_TABLE_SQL = """
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
"""

INDEXES = [
    ("idx_ruc", "CREATE INDEX IF NOT EXISTS idx_ruc ON contribuyentes(ruc)"),
    ("idx_estado", "CREATE INDEX IF NOT EXISTS idx_estado ON contribuyentes(estado_contribuyente)"),
    ("idx_condicion", "CREATE INDEX IF NOT EXISTS idx_condicion ON contribuyentes(condicion_domicilio)"),
    ("idx_ubigeo", "CREATE INDEX IF NOT EXISTS idx_ubigeo ON contribuyentes(ubigeo)"),
    ("idx_nombre", "CREATE INDEX IF NOT EXISTS idx_nombre ON contribuyentes(nombre_razon_social)"),
]
