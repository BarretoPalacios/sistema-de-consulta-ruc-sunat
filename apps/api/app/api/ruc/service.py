import time
import re
from typing import Optional
from datetime import datetime
from cachetools import TTLCache

from packages.database.connection import get_connection
from packages.config.settings import settings
from apps.api.app.core.config import api_settings
from apps.api.app.api.ruc.model import ContribuyenteOut


class RUCService:
    def __init__(self):
        self.cache: TTLCache = TTLCache(maxsize=settings.CACHE_SIZE, ttl=settings.CACHE_TTL)
        self.stats = {
            "total_consultas": 0,
            "consultas_exitosas": 0,
            "consultas_fallidas": 0,
            "tiempo_total": 0.0,
            "consultas_api": 0,
            "consultas_web": 0,
        }

    def _connect(self):
        return get_connection()

    def _clean_ruc(self, ruc: str) -> str:
        cleaned = re.sub(r"[\s\-\.]", "", ruc)
        if len(cleaned) != 11 or not cleaned.isdigit():
            raise ValueError(f"RUC inválido: {ruc}. Debe tener 11 dígitos.")
        return cleaned

    def buscar_por_ruc(self, ruc: str, source: str = "api") -> dict:
        start = time.time()
        self.stats["total_consultas"] += 1
        if source == "api":
            self.stats["consultas_api"] += 1
        else:
            self.stats["consultas_web"] += 1

        try:
            ruc_clean = self._clean_ruc(ruc)

            cache_key = f"ruc:{ruc_clean}"
            if cache_key in self.cache:
                result = self.cache[cache_key]
                elapsed = time.time() - start
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                return result

            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM contribuyentes WHERE ruc = ? LIMIT 1", (ruc_clean,))
            row = cursor.fetchone()
            conn.close()

            if row:
                contribuyente = ContribuyenteOut.from_row(row)
                result = {
                    "success": True,
                    "data": contribuyente.model_dump(),
                    "timestamp": datetime.now().isoformat(),
                    "response_time": time.time() - start,
                }
                self.cache[cache_key] = result
                elapsed = time.time() - start
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                return result

            elapsed = time.time() - start
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            return {
                "success": False,
                "error": "RUC no encontrado",
                "ruc": ruc_clean,
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed,
            }

        except ValueError as e:
            elapsed = time.time() - start
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed,
            }
        except Exception as e:
            elapsed = time.time() - start
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            return {
                "success": False,
                "error": f"Error interno: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed,
            }

    def obtener_estadisticas(self) -> dict:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            total_registros = cursor.fetchone()[0]
            conn.close()

            avg_time = 0
            if self.stats["total_consultas"] > 0:
                avg_time = self.stats["tiempo_total"] / self.stats["total_consultas"] * 1000

            success_rate = 0
            if self.stats["total_consultas"] > 0:
                success_rate = (self.stats["consultas_exitosas"] / self.stats["total_consultas"]) * 100

            return {
                "success": True,
                "data": {
                    "estadisticas": {
                        "total_consultas": self.stats["total_consultas"],
                        "consultas_exitosas": self.stats["consultas_exitosas"],
                        "consultas_fallidas": self.stats["consultas_fallidas"],
                        "tasa_exito": f"{success_rate:.1f}%",
                        "consultas_api": self.stats["consultas_api"],
                        "consultas_web": self.stats["consultas_web"],
                        "tiempo_promedio_ms": f"{avg_time:.1f}",
                        "tiempo_total_segundos": f"{self.stats['tiempo_total']:.1f}",
                    },
                    "base_datos": {"total_registros": total_registros},
                    "cache": {"size": len(self.cache), "max_size": self.cache.maxsize},
                    "timestamp": datetime.now().isoformat(),
                },
            }
        except Exception as e:
            return {"success": False, "error": str(e), "timestamp": datetime.now().isoformat()}

    def obtener_status(self) -> dict:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            total = cursor.fetchone()[0]
            conn.close()
            return {"success": True, "data": {"total_registros": total, "status": "healthy"}}
        except Exception as e:
            return {"success": False, "error": str(e), "status": "degraded"}

    def exportar(self, ruc: str, formato: str = "json") -> dict:
        result = self.buscar_por_ruc(ruc, source="api")
        if not result["success"]:
            return result

        data = result["data"]
        if formato == "json":
            return {"success": True, "format": "json", "filename": f"ruc_{ruc}.json", "data": data}
        elif formato == "csv":
            headers = ",".join(data.keys())
            values = ",".join(str(v) for v in data.values())
            return {"success": True, "format": "csv", "filename": f"ruc_{ruc}.csv", "data": f"{headers}\n{values}"}
        elif formato == "txt":
            lines = [f"{k}: {v}" for k, v in data.items()]
            return {"success": True, "format": "txt", "filename": f"ruc_{ruc}.txt", "data": "\n".join(lines)}
        else:
            return {"success": False, "error": f"Formato '{formato}' no soportado"}
