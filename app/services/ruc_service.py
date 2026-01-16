import sqlite3
import time
import re
from typing import Optional, List, Dict, Any
from datetime import datetime
from app.models.contribuyente import Contribuyente
from app.config import settings

class RUCService:
    """Servicio para búsqueda de RUCs"""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern"""
        if cls._instance is None:
            cls._instance = super(RUCService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.db_path = settings.DATABASE_URL
        self.conn = None
        self.cursor = None
        self._connect()
        
        # Estadísticas
        self.stats = {
            "total_consultas": 0,
            "consultas_exitosas": 0,
            "consultas_fallidas": 0,
            "tiempo_total": 0.0,
            "consultas_api": 0,
            "consultas_web": 0
        }
        
        # Cache simple en memoria
        self.cache = {}
        self.cache_size = settings.CACHE_SIZE
        
        self._initialized = True
    
    def _connect(self):
        """Conectar a la base de datos"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            print(f"✅ Conectado a base de datos: {self.db_path}")
            
            # Verificar que existe la tabla
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contribuyentes'")
            if not self.cursor.fetchone():
                raise Exception("❌ No se encuentra la tabla 'contribuyentes'")
                
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            raise
    
    def _clean_ruc(self, ruc: str) -> str:
        """Limpiar y validar RUC"""
        cleaned = re.sub(r'[\s\-\.]', '', ruc)
        
        if len(cleaned) != 11 or not cleaned.isdigit():
            raise ValueError(f"RUC inválido: {ruc}. Debe tener 11 dígitos.")
        
        return cleaned
    
    def buscar_por_ruc(self, ruc: str, source: str = "api") -> Optional[Dict[str, Any]]:
        """Buscar contribuyente por RUC"""
        start_time = time.time()
        self.stats["total_consultas"] += 1
        
        if source == "api":
            self.stats["consultas_api"] += 1
        else:
            self.stats["consultas_web"] += 1
        
        try:
            ruc_clean = self._clean_ruc(ruc)
            
            # Verificar cache
            cache_key = f"ruc:{ruc_clean}"
            if cache_key in self.cache:
                result = self.cache[cache_key]
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                return result
            
            # Buscar en base de datos
            query = """
            SELECT * FROM contribuyentes 
            WHERE ruc = ? 
            LIMIT 1
            """
            
            self.cursor.execute(query, (ruc_clean,))
            row = self.cursor.fetchone()
            
            if row:
                contribuyente = Contribuyente.from_db_row(row)
                result = {
                    "success": True,
                    "data": contribuyente.to_dict(),
                    "timestamp": datetime.now().isoformat(),
                    "response_time": time.time() - start_time
                }
                
                # Guardar en cache
                if len(self.cache) >= self.cache_size:
                    self.cache.pop(next(iter(self.cache)))
                self.cache[cache_key] = result
                
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                
                return result
            else:
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_fallidas"] += 1
                
                return {
                    "success": False,
                    "error": "RUC no encontrado",
                    "ruc": ruc_clean,
                    "timestamp": datetime.now().isoformat(),
                    "response_time": elapsed
                }
                
        except ValueError as e:
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
        except Exception as e:
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            
            return {
                "success": False,
                "error": f"Error interno: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
    
    def buscar_por_nombre(self, nombre: str, limit: int = 10) -> Dict[str, Any]:
        """Buscar por nombre o razón social"""
        start_time = time.time()
        self.stats["total_consultas"] += 1
        
        try:
            if len(nombre.strip()) < 3:
                return {
                    "success": False,
                    "error": "Ingrese al menos 3 caracteres",
                    "timestamp": datetime.now().isoformat(),
                    "response_time": time.time() - start_time
                }
            
            search_term = f"%{nombre.strip().upper()}%"
            
            query = """
            SELECT * FROM contribuyentes 
            WHERE UPPER(nombre_razon_social) LIKE ? 
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (search_term, limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row).to_dict() for row in rows]
            
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_exitosas"] += 1
            
            return {
                "success": True,
                "count": len(resultados),
                "data": resultados,
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
    
    def buscar_por_departamento(self, departamento: str, limit: int = 20) -> Dict[str, Any]:
        """Buscar por departamento"""
        start_time = time.time()
        self.stats["total_consultas"] += 1
        
        try:
            if len(departamento) != 2 or not departamento.isdigit():
                return {
                    "success": False,
                    "error": "Código de departamento debe ser 2 dígitos",
                    "timestamp": datetime.now().isoformat(),
                    "response_time": time.time() - start_time
                }
            
            query = """
            SELECT * FROM contribuyentes 
            WHERE ubigeo LIKE ? || '%'
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (departamento, limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row).to_dict() for row in rows]
            
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_exitosas"] += 1
            
            return {
                "success": True,
                "count": len(resultados),
                "departamento": departamento,
                "data": resultados,
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
    
    def buscar_por_estado(self, estado: str, limit: int = 20) -> Dict[str, Any]:
        """Buscar por estado"""
        start_time = time.time()
        self.stats["total_consultas"] += 1
        
        try:
            query = """
            SELECT * FROM contribuyentes 
            WHERE estado_contribuyente = ? 
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (estado.upper(), limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row).to_dict() for row in rows]
            
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_exitosas"] += 1
            
            return {
                "success": True,
                "count": len(resultados),
                "estado": estado.upper(),
                "data": resultados,
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.stats["tiempo_total"] += elapsed
            self.stats["consultas_fallidas"] += 1
            
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "response_time": elapsed
            }
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio"""
        try:
            # Obtener total de registros
            self.cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            total_registros = self.cursor.fetchone()[0]
            
            # Calcular promedios
            avg_time = 0
            if self.stats["total_consultas"] > 0:
                avg_time = self.stats["tiempo_total"] / self.stats["total_consultas"] * 1000
            
            success_rate = 0
            if self.stats["total_consultas"] > 0:
                success_rate = (self.stats["consultas_exitosas"] / self.stats["total_consultas"]) * 100
            
            cache_info = {
                "size": len(self.cache),
                "max_size": self.cache_size
            }
            
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
                        "tiempo_total_segundos": f"{self.stats['tiempo_total']:.1f}"
                    },
                    "base_datos": {
                        "total_registros": total_registros,
                        "ruta": self.db_path
                    },
                    "cache": cache_info,
                    "timestamp": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def obtener_departamentos(self) -> Dict[str, Any]:
        """Obtener lista de departamentos disponibles"""
        try:
            query = """
            SELECT DISTINCT SUBSTR(ubigeo, 1, 2) as departamento_code,
                   COUNT(*) as count
            FROM contribuyentes
            WHERE ubigeo IS NOT NULL AND ubigeo != '' AND ubigeo != '-'
            GROUP BY SUBSTR(ubigeo, 1, 2)
            ORDER BY departamento_code
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            departamentos = [
                {"codigo": row[0], "cantidad": row[1]}
                for row in rows if row[0] and len(row[0]) == 2
            ]
            
            return {
                "success": True,
                "count": len(departamentos),
                "data": departamentos,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def obtener_estados(self) -> Dict[str, Any]:
        """Obtener lista de estados disponibles"""
        try:
            query = """
            SELECT DISTINCT estado_contribuyente as estado,
                   COUNT(*) as count
            FROM contribuyentes
            WHERE estado_contribuyente IS NOT NULL 
            GROUP BY estado_contribuyente
            ORDER BY count DESC
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            estados = [
                {"estado": row[0], "cantidad": row[1]}
                for row in rows if row[0]
            ]
            
            return {
                "success": True,
                "count": len(estados),
                "data": estados,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def close(self):
        """Cerrar conexión"""
        if self.conn:
            self.conn.close()