import zipfile


def detect_encoding(filepath: str) -> str:
    encodings = ["latin-1", "iso-8859-1", "cp1252", "utf-8"]
    for encoding in encodings:
        try:
            with open(filepath, "r", encoding=encoding) as f:
                for _ in range(5):
                    line = f.readline()
                    if "|" in line:
                        return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def detect_encoding_from_zip(zip_path: str) -> str:
    encodings = ["latin-1", "iso-8859-1", "cp1252", "utf-8"]
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if n.endswith(".txt")]
            if not names:
                return "latin-1"
            with zf.open(names[0]) as raw:
                head = raw.read(2048)
        for encoding in encodings:
            try:
                decoded = head.decode(encoding)
                if "|" in decoded:
                    return encoding
            except UnicodeDecodeError:
                continue
    except Exception:
        pass
    return "latin-1"


def clean_value(value: str) -> str | None:
    if not value or value == "-" or value.strip() == "":
        return None
    cleaned = value.strip()
    replacements = {
        "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó", "Ãº": "ú",
        "Ã±": "ñ", "Â": "", "â€“": "-", "â€œ": '"', "â€": '"',
        "â€¢": "-", "â€¦": "..."
    }
    for wrong, correct in replacements.items():
        cleaned = cleaned.replace(wrong, correct)
    return cleaned
