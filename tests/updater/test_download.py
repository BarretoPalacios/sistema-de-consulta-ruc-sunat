import os
import zipfile
import tempfile
from unittest.mock import patch, Mock
import pytest

from packages.utils.files import remove_if_exists
from apps.updater.services.extract_service import extract_txt_from_zip


def _create_test_zip(zip_path, txt_content="HEADER\n10452159428|JUAN PEREZ|ACTIVO|HABIDO|150101|AV|LOS OLIVOS|||||||||\n", txt_name="padron_reducido_ruc.txt"):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(txt_name, txt_content)
    return zip_path


class TestDownloadService:
    @patch("requests.get")
    def test_download_zip_success(self, mock_get):
        from apps.updater.services.download_service import download_zip
        mock_response = Mock()
        mock_response.iter_content.return_value = [b"zipdata1\n", b"zipdata2\n"]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as td:
            result = download_zip("https://ejemplo.com/data.zip", td)
            assert result is not None
            assert result.endswith(".zip")
            assert os.path.exists(result)

    @patch("requests.get")
    def test_download_zip_network_error(self, mock_get):
        from apps.updater.services.download_service import download_zip
        mock_get.side_effect = Exception("Network error")
        with pytest.raises(Exception):
            download_zip("https://ejemplo.com/data.zip", "/tmp/fake")

    def test_extract_txt_from_zip_success(self):
        with tempfile.TemporaryDirectory() as td:
            zip_path = os.path.join(td, "test.zip")
            _create_test_zip(zip_path)
            txt_path = extract_txt_from_zip(zip_path, td)
            assert txt_path is not None
            assert txt_path.endswith(".txt")
            assert os.path.exists(txt_path)

    def test_extract_txt_from_zip_no_txt(self):
        with tempfile.TemporaryDirectory() as td:
            zip_path = os.path.join(td, "test.zip")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("datos.csv", "a,b,c\n1,2,3\n")
            with pytest.raises(Exception):
                extract_txt_from_zip(zip_path, td)

    def test_extract_txt_from_zip_corrupt(self):
        with tempfile.TemporaryDirectory() as td:
            zip_path = os.path.join(td, "corrupt.zip")
            with open(zip_path, "wb") as f:
                f.write(b"not a zip file")
            with pytest.raises(Exception):
                extract_txt_from_zip(zip_path, td)

    def test_cleanup_removes_dir(self):
        with tempfile.TemporaryDirectory() as td:
            test_file = os.path.join(td, "test.txt")
            with open(test_file, "w") as f:
                f.write("data")
            assert os.path.exists(td)
            remove_if_exists(td)
            assert not os.path.exists(td)

    def test_detect_encoding_latin1(self):
        from packages.utils.encodings import detect_encoding
        with tempfile.NamedTemporaryFile(mode="w", encoding="latin-1", suffix=".txt", delete=False) as f:
            f.write("HEADER|COL2\n10452159428|JUAN PEREZ|ACTIVO\n")
            fname = f.name
        try:
            encoding = detect_encoding(fname)
            assert encoding == "latin-1"
        finally:
            os.unlink(fname)

    def test_detect_encoding_fallback(self):
        from packages.utils.encodings import detect_encoding
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".txt", delete=False) as f:
            f.write("SIN PIPES\n")
            fname = f.name
        try:
            encoding = detect_encoding(fname)
            assert encoding == "latin-1"
        finally:
            os.unlink(fname)
