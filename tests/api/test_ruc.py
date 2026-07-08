import pytest
from fastapi.testclient import TestClient
from apps.api.app.core.config import api_settings


class TestRucAPI:
    def test_health(self, test_client: TestClient):
        response = test_client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_home_page(self, test_client: TestClient):
        response = test_client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_docs_page(self, test_client: TestClient):
        response = test_client.get("/docs")
        assert response.status_code == 200

    def test_stats_page(self, test_client: TestClient):
        response = test_client.get("/estadisticas")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_buscar_ruc_no_token(self, test_client: TestClient):
        response = test_client.get("/api/ruc/10452159428")
        assert response.status_code == 403

    def test_buscar_ruc_invalid_token(self, test_client: TestClient):
        response = test_client.get(
            "/api/ruc/10452159428",
            headers={"Authorization": "Bearer invalid-token"}
        )
        assert response.status_code == 401

    def test_buscar_ruc_valid_token(self, test_client: TestClient):
        response = test_client.get(
            "/api/ruc/10452159428",
            headers={"Authorization": f"Bearer {api_settings.API_TOKEN}"}
        )
        assert response.status_code in (200, 404)

    def test_buscar_ruc_invalid_input(self, test_client: TestClient):
        response = test_client.get(
            "/api/ruc/123",
            headers={"Authorization": f"Bearer {api_settings.API_TOKEN}"}
        )
        assert response.status_code in (404, 422)

    def test_stats_no_token(self, test_client: TestClient):
        response = test_client.get("/api/stats")
        assert response.status_code == 403

    def test_stats_with_token(self, test_client: TestClient):
        response = test_client.get(
            "/api/stats",
            headers={"Authorization": f"Bearer {api_settings.API_TOKEN}"}
        )
        assert response.status_code in (200, 500)

    def test_export_no_auth(self, test_client: TestClient):
        response = test_client.post("/api/export", json={"ruc": "10452159428", "format": "json"})
        assert response.status_code == 403

    def test_export_with_token(self, test_client: TestClient):
        response = test_client.post(
            "/api/export",
            json={"ruc": "10452159428", "format": "json"},
            headers={"Authorization": f"Bearer {api_settings.API_TOKEN}"}
        )
        assert response.status_code in (200, 404)
