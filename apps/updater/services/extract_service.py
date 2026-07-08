import os
import zipfile

from packages.exceptions import ParseError
from packages.logger.logger import get_logger
from packages.utils.files import ensure_dir

logger = get_logger("updater.extract")


def extract_txt_from_zip(zip_path: str, temp_dir: str) -> str:
    try:
        ensure_dir(temp_dir)
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if n.endswith(".txt")]
            if not names:
                raise ParseError("No se encontró ningún .txt dentro del ZIP")

            txt_name = names[0]
            if len(names) > 1:
                logger.warning(f"Múltiples TXT en ZIP, usando el primero: {txt_name}")

            zf.extract(txt_name, temp_dir)
            extracted_path = os.path.join(temp_dir, txt_name)

            if not os.path.exists(extracted_path):
                actual = os.path.join(temp_dir, os.path.basename(txt_name))
                if os.path.exists(actual):
                    extracted_path = actual

            file_size = os.path.getsize(extracted_path)
            logger.info(f"TXT extraído: {extracted_path} ({file_size / 1024 / 1024:.1f} MB)")
            return extracted_path
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Error al extraer TXT del ZIP: {e}")
