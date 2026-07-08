from typing import List, Optional

from packages.logger.logger import get_logger
from packages.utils.encodings import clean_value

logger = get_logger("updater.parse")


def parse_line(line: str) -> Optional[List[Optional[str]]]:
    try:
        cleaned = line.strip().rstrip("|")
        if not cleaned:
            return None
        values = cleaned.split("|")
        if len(values) < 15:
            values.extend([None] * (15 - len(values)))
        elif len(values) > 15:
            extra = values[15:]
            values = values[:15]
            if extra and values[1]:
                values[1] = values[1] + " " + " ".join(extra)
        return [clean_value(v) if v is not None else None for v in values]
    except Exception:
        return None
