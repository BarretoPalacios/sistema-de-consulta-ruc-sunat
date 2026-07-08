import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import argparse

from packages.logger.logger import setup_logging, get_logger
from packages.config.settings import settings
from apps.updater.services.update_service import run_update


def main():
    parser = argparse.ArgumentParser(description="Updater del padrón SUNAT")
    parser.add_argument("--run", action="store_true", help="Ejecutar actualización")
    parser.add_argument("--force", action="store_true", help="Forzar descarga aunque no haya cambios")
    parser.add_argument("--check", action="store_true", help="Verificar si hay cambios disponibles")
    parser.add_argument("--status", action="store_true", help="Mostrar estado de la base de datos")

    args = parser.parse_args()

    setup_logging("updater", settings.LOG_DIR, settings.LOG_LEVEL)
    logger = get_logger("updater")

    data_dir = os.path.dirname(os.path.abspath(settings.DATABASE_URL)) or "data"
    temp_dir = os.path.join(data_dir, "temp_updater")

    url = os.getenv("SCRAPER_URL", "http://www2.sunat.gob.pe/padron_reducido_ruc.zip")

    if args.run or args.force:
        logger.info("Iniciando actualización del padrón SUNAT...")
        result = run_update(url, data_dir, temp_dir, force=args.force)
        if result["success"]:
            if result.get("updated"):
                logger.info(f"✓ Padrón actualizado: {result['records']} registros en {result['elapsed_seconds']}s")
            else:
                logger.info("✓ Sin cambios — base actualizada")
        else:
            logger.error(f"✗ Error: {result['error']}")
            sys.exit(1)

    elif args.check:
        from apps.updater.services.download_service import check_remote
        metadata_path = os.path.join(data_dir, "metadata.json")
        needs_update, info = check_remote(url, metadata_path)
        if needs_update:
            logger.info("Hay cambios disponibles — ejecute con --run")
        else:
            logger.info("No hay cambios — base actualizada")

    elif args.status:
        import sqlite3
        db_path = settings.DATABASE_URL
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            count = cursor.fetchone()[0]
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [r[0] for r in cursor.fetchall()]
            conn.close()
            logger.info(f"Base de datos: {db_path}")
            logger.info(f"Registros: {count:,}")
            logger.info(f"Índices: {', '.join(indexes) if indexes else 'ninguno'}")
        else:
            logger.warning(f"Base de datos no encontrada: {db_path}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
