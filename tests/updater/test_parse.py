import os
import sqlite3
import pytest

from packages.utils.encodings import clean_value, detect_encoding
from apps.updater.services.parse_service import parse_line


class TestParseService:
    def test_clean_value_none(self):
        assert clean_value("") is None
        assert clean_value("-") is None
        assert clean_value("   ") is None

    def test_clean_value_normal(self):
        assert clean_value("  JUAN PEREZ  ") == "JUAN PEREZ"

    def test_parse_line_valid(self):
        line = "10452159428|JUAN PEREZ|ACTIVO|HABIDO|150101|AV|LOS OLIVOS|||||||||"
        result = parse_line(line)
        assert result is not None
        assert result[0] == "10452159428"
        assert result[1] == "JUAN PEREZ"
        assert result[2] == "ACTIVO"

    def test_parse_line_short(self):
        line = "10452159428|JUAN PEREZ"
        result = parse_line(line)
        assert result is not None
        assert len(result) == 15

    def test_parse_line_long(self):
        line = "10452159428|JUAN|ACTIVO|HABIDO|150101|AV|LOS OLIVOS|||||||||extra1|extra2"
        result = parse_line(line)
        assert result is not None
        assert "extra1" in (result[1] or "")

    def test_parse_line_empty(self):
        assert parse_line("") is None
        assert parse_line("   ") is None

    def test_detect_encoding(self):
        filepath = "test_encoding.txt"
        with open(filepath, "w", encoding="latin-1") as f:
            f.write("10452159428|JUAN PEREZ|ACTIVO\n")
        try:
            enc = detect_encoding(filepath)
            assert enc in ("latin-1", "iso-8859-1", "cp1252")
        finally:
            os.remove(filepath)

    def test_load_to_db_with_text_data(self):
        from apps.updater.services.load_service import load_to_db

        txt_data = "HEADER\n10452159428|JUAN PEREZ|ACTIVO|HABIDO|150101|AV|LOS OLIVOS|||||||||\n"
        filepath = "test_padron.txt"
        with open(filepath, "w", encoding="latin-1") as f:
            f.write(txt_data)

        try:
            records, errors = load_to_db(filepath, ":memory:")
            assert records == 1
            assert errors == 0
        finally:
            os.remove(filepath)
