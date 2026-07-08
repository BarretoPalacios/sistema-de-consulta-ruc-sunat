# Sistema de Consulta RUC SUNAT

API REST + Web UI para consultar el padrón reducido de contribuyentes de SUNAT (Perú). Open Source, Docker-first, resistente a fallos durante actualizaciones.

---

## Stack

| Capa | Tecnología |
|---|---|
| Framework | FastAPI + uvicorn |
| Frontend | Jinja2 + Tailwind CSS (CDN) |
| Base de datos | SQLite (WAL) |
| Cache | TTLCache (cachetools) |
| Auth | Bearer Token estático |
| Rate limiting | slowapi |
| Actualización | Docker + host CRON |

## Estructura

```
sistema-de-consulta-ruc-sunat/
├── apps/
│   ├── api/                    # FastAPI — consultas, auth, docs, frontend
│   │   ├── main.py             # Entrypoint: uvicorn apps.api.main:app
│   │   └── app/
│   │       ├── core/           # config, security, limiter, dependencies
│   │       ├── api/ruc/        # router, service, model, schema
│   │       └── web/            # routes + Jinja2 templates
│   └── updater/                # CLI — ETL del padrón SUNAT
│       ├── main.py             # Entrypoint: python apps/updater/main.py --run
│       └── services/           # download, extract, parse, load, index, publish
├── packages/                   # Código compartido entre apps
│   ├── config/                 # Settings base (pydantic-settings)
│   ├── database/               # Conexión SQLite, helpers bulk insert
│   ├── logger/                 # Logging con rotación (10 MB, 5 rotaciones)
│   ├── constants/              # Columnas, índices, nombres de tablas
│   ├── utils/                  # Archivos, hashes, detección de encoding
│   └── exceptions/             # UpdateError, DownloadError, ParseError...
├── tests/                      # pytest (28 tests)
├── data/                       # contribuyentes.db + metadata.json
├── docker/                     # Dockerfile.api + Dockerfile.updater
├── requirements/               # Dependencias separadas por app
├── docker-compose.yml          # Único archivo para levantar todo
├── .env.example                # Template de configuración
├── pyproject.toml              # Metadatos del proyecto
└── README.md
```

---

## Inicio rápido

### Desarrollo local

```bash
# 1. Clonar
git clone https://github.com/BarretoPalacios/sistema-de-consulta-ruc-sunat.git
cd sistema-de-consulta-ruc-sunat

# 2. Entorno virtual
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

# 3. Dependencias
pip install -r requirements/dev.txt

# 4. Configurar
cp .env.example .env
# Editar API_TOKEN en .env con un token seguro

# 5. Obtener BD (descarga desde SUNAT, ~30 min)
python apps/updater/main.py --run

# 6. Iniciar API (hot-reload activado)
python apps/api/main.py

# Abrir http://localhost:8000
```

### Producción (Docker)

```bash
# 1. Clonar en el servidor
git clone https://github.com/BarretoPalacios/sistema-de-consulta-ruc-sunat.git /opt/sistema-de-consulta-ruc-sunat
cd /opt/sistema-de-consulta-ruc-sunat

# 2. Configurar
cp .env.example .env
# Editar API_TOKEN en .env

# 3. Iniciar API
docker compose up -d

# 4. Descargar padrón (solo primera vez, ~30 min)
docker compose run --rm updater

# 5. Programar actualización diaria a las 3 AM
crontab -e
# Agregar: 0 3 * * * cd /opt/sistema-de-consulta-ruc-sunat && docker compose run --rm updater

# 6. Ver logs
docker compose logs -f
```

### Comandos útiles

```bash
docker compose up -d          # Iniciar API en segundo plano
docker compose down           # Detener API
docker compose logs -f        # Ver logs en tiempo real
docker compose run --rm updater   # Ejecutar actualización manual
```

---

## API

### Autenticación

Todas las rutas protegidas requieren el header:

```
Authorization: Bearer <API_TOKEN>
```

El token se define en `.env`.

### Endpoints

| Método | Ruta | Descripción | Auth |
|---|---|---|---|
| `GET` | `/health` | Health check | No |
| `GET` | `/` | Web UI — inicio | No |
| `GET` | `/detalle/{ruc}` | Web UI — resultado | No |
| `GET` | `/docs` | Web UI — documentación | No |
| `GET` | `/estadisticas` | Web UI — estadísticas | No |
| `GET` | `/api/ruc/{ruc}` | Consultar contribuyente por RUC | Sí |
| `GET` | `/api/stats` | Estadísticas de uso y BD | Sí |
| `GET` | `/api/status` | Estado del sistema | Sí |
| `POST` | `/api/export` | Exportar (JSON/CSV/TXT) | Sí |

### Ejemplo

```bash
curl -X GET "http://localhost:8000/api/ruc/10452159428" \
  -H "Authorization: Bearer TU_TOKEN"
```

```json
{
  "success": true,
  "data": {
    "ruc": "10452159428",
    "nombre_razon_social": "JUAN PEREZ",
    "estado_contribuyente": "ACTIVO",
    "condicion_domicilio": "HABIDO",
    "ubigeo": "150101",
    "direccion_completa": "AV. LOS OLIVOS 123",
    "tipo_via": "AV",
    "nombre_via": "LOS OLIVOS",
    "numero": "123"
  },
  "timestamp": "2026-07-08T12:00:00",
  "response_time": 0.015
}
```

---

## Updater (CLI)

```
python apps/updater/main.py --run       # Actualizar padrón
python apps/updater/main.py --force     # Forzar descarga (ignorar cache)
python apps/updater/main.py --check     # Verificar si hay cambios
python apps/updater/main.py --status    # Mostrar estado de la BD
```

En Docker:

```
docker compose run --rm updater                    # Equivale a --run
docker compose run --rm updater python apps/updater/main.py --force
docker compose run --rm updater python apps/updater/main.py --status
```

### Estrategia de actualización atómica

```
1. HEAD request → comparar ETag / Last-Modified
2. Descargar ZIP (streaming, reintentos)
3. Extraer TXT
4. Crear contribuyentes_new.db
5. Batch insert (30k registros, PRAGMAs optimizados)
6. Crear índices (5 índices)
7. Validar (contar registros)
8. Publicar (atómico):
   a. contribuyentes.db → contribuyentes_old.db
   b. contribuyentes_new.db → contribuyentes.db
   c. eliminar contribuyentes_old.db
9. Guardar metadata.json (ETag para próxima vez)
10. Limpiar temporales
```

La API nunca se queda sin datos. `contribuyentes.db` solo se reemplaza al final si todo salió bien.

---

## Tests

```bash
pytest -v
```

28 tests que cubren:

- Endpoints API (auth, validación, respuestas HTTP)
- Parseo de líneas del padrón
- Extracción de ZIP
- Detección de encoding
- Carga a base de datos
- Limpieza de archivos

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `API_TOKEN` | — | Token de autenticación Bearer |
| `API_TITLE` | `Sistema de Búsqueda de RUC` | Título de la API |
| `API_VERSION` | `1.0.0` | Versión de la API |
| `API_DESCRIPTION` | `API para consulta del padrón SUNAT` | Descripción de la API |
| `WEB_TITLE` | `Búsqueda RUC SUNAT` | Título de la web UI |
| `WEB_DESCRIPTION` | `Sistema de consulta del padrón de contribuyentes` | Descripción de la web UI |
| `DATABASE_URL` | `data/contribuyentes.db` | Ruta a la BD SQLite |
| `CACHE_SIZE` | `10000` | Máximo de entradas en caché |
| `CACHE_TTL` | `3600` | TTL de caché en segundos |
| `RATE_LIMIT_PER_MINUTE` | `100` | Máximo de requests por minuto |
| `CORS_ORIGINS` | `*` | Orígenes permitidos (CORS) |
| `SCRAPER_URL` | URL de SUNAT | URL del archivo ZIP del padrón |
| `LOG_LEVEL` | `INFO` | Nivel de logging |
| `LOG_DIR` | `logs` | Directorio de logs |

---

## Roadmap

- **v1** — Consulta por RUC, Web UI, actualización atómica ← _actual_
- **v2** — Búsqueda por razón social (solo prefijo), mejoras de rendimiento
- **v3** — Métricas, observabilidad, exportación masiva

---

## Cómo contribuir

1. Fork el repositorio
2. Crear rama: `git checkout -b feature/mi-cambio`
3. Hacer cambios con tests
4. Verificar: `ruff check . && pytest`
5. Crear Pull Request

### Lineamientos

- Sin patrones complejos (no DDD, no CQRS, no Event Sourcing)
- Código compartido en `packages/`
- La API nunca debe perder disponibilidad
- Una actualización fallida nunca debe afectar la BD en uso

---

## Licencia

MIT
