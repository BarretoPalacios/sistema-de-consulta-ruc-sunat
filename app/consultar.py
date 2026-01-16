import sqlite3
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import time
from datetime import datetime
import json
import re

@dataclass
class Contribuyente:
    """Modelo de datos para contribuyentes"""
    ruc: str
    nombre_razon_social: Optional[str] = None
    estado_contribuyente: Optional[str] = None
    condicion_domicilio: Optional[str] = None
    ubigeo: Optional[str] = None
    tipo_via: Optional[str] = None
    nombre_via: Optional[str] = None
    codigo_zona: Optional[str] = None
    tipo_zona: Optional[str] = None
    numero: Optional[str] = None
    interior: Optional[str] = None
    lote: Optional[str] = None
    departamento: Optional[str] = None
    manzana: Optional[str] = None
    kilometro: Optional[str] = None
    
    @property
    def direccion_completa(self) -> str:
        """Generar dirección completa"""
        partes = []
        if self.tipo_via and self.nombre_via:
            partes.append(f"{self.tipo_via} {self.nombre_via}")
        if self.numero:
            partes.append(f"NRO {self.numero}")
        if self.interior:
            partes.append(f"INT {self.interior}")
        if self.lote:
            partes.append(f"LOTE {self.lote}")
        if self.departamento:
            partes.append(f"DPTO {self.departamento}")
        if self.manzana:
            partes.append(f"MZA {self.manzana}")
        if self.kilometro:
            partes.append(f"KM {self.kilometro}")
        
        return ", ".join(filter(None, partes)) if partes else "SIN DIRECCIÓN"
    
    @property
    def direccion_simple(self) -> str:
        """Dirección simplificada"""
        if self.tipo_via and self.nombre_via and self.numero:
            return f"{self.tipo_via} {self.nombre_via} {self.numero}"
        return self.direccion_completa
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario"""
        return {
            "ruc": self.ruc,
            "nombre_razon_social": self.nombre_razon_social,
            "estado_contribuyente": self.estado_contribuyente,
            "condicion_domicilio": self.condicion_domicilio,
            "ubigeo": self.ubigeo,
            "direccion_completa": self.direccion_completa,
            "direccion_simple": self.direccion_simple,
            "tipo_via": self.tipo_via,
            "nombre_via": self.nombre_via,
            "codigo_zona": self.codigo_zona,
            "tipo_zona": self.tipo_zona,
            "numero": self.numero,
            "interior": self.interior,
            "lote": self.lote,
            "departamento": self.departamento,
            "manzana": self.manzana,
            "kilometro": self.kilometro
        }
    
    def to_json(self) -> str:
        """Convertir a JSON"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_db_row(cls, row) -> 'Contribuyente':
        """Crear instancia desde fila de base de datos"""
        return cls(
            ruc=row[0] if row[0] else "",
            nombre_razon_social=row[1],
            estado_contribuyente=row[2],
            condicion_domicilio=row[3],
            ubigeo=row[4],
            tipo_via=row[5],
            nombre_via=row[6],
            codigo_zona=row[7],
            tipo_zona=row[8],
            numero=row[9],
            interior=row[10],
            lote=row[11],
            departamento=row[12],
            manzana=row[13],
            kilometro=row[14]
        )

class RUCService:
    """Servicio para búsqueda de RUCs"""
    
    def __init__(self, db_path: str = "contribuyentes.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self._connect()
        
        # Estadísticas
        self.stats = {
            "total_consultas": 0,
            "consultas_exitosas": 0,
            "consultas_fallidas": 0,
            "tiempo_total": 0.0,
            "cache_hits": 0,
            "cache_misses": 0
        }
        
        # Cache simple en memoria
        self.cache = {}
        self.cache_size = 10000  # Mantener 10,000 registros en cache
        
    def _connect(self):
        """Conectar a la base de datos"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Para acceso por nombre de columna
            self.cursor = self.conn.cursor()
            print(f"✅ Conectado a base de datos: {self.db_path}")
            
            # Verificar que existe la tabla
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contribuyentes'")
            if not self.cursor.fetchone():
                raise Exception("❌ No se encuentra la tabla 'contribuyentes'")
                
            # Contar registros
            self.cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            total = self.cursor.fetchone()[0]
            print(f"📊 Total registros en base de datos: {total:,}")
            
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            raise
    
    def _clean_ruc(self, ruc: str) -> str:
        """Limpiar y validar RUC"""
        # Remover espacios, guiones, puntos
        cleaned = re.sub(r'[\s\-\.]', '', ruc)
        
        # Validar longitud (RUC peruano: 11 dígitos)
        if len(cleaned) != 11 or not cleaned.isdigit():
            raise ValueError(f"RUC inválido: {ruc}. Debe tener 11 dígitos.")
        
        return cleaned
    
    def buscar_por_ruc(self, ruc: str) -> Optional[Contribuyente]:
        """Buscar contribuyente por RUC"""
        start_time = time.time()
        self.stats["total_consultas"] += 1
        
        try:
            # Limpiar RUC
            ruc_clean = self._clean_ruc(ruc)
            
            # Verificar cache primero
            if ruc_clean in self.cache:
                self.stats["cache_hits"] += 1
                result = self.cache[ruc_clean]
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                print(f"⚡ RUC {ruc_clean} encontrado en cache ({elapsed*1000:.1f}ms)")
                return result
            
            self.stats["cache_misses"] += 1
            
            # Buscar en base de datos
            query = """
            SELECT * FROM contribuyentes 
            WHERE ruc = ? 
            LIMIT 1
            """
            
            self.cursor.execute(query, (ruc_clean,))
            row = self.cursor.fetchone()
            
            if row:
                # Convertir a objeto Contribuyente
                contribuyente = Contribuyente.from_db_row(row)
                
                # Guardar en cache
                if len(self.cache) >= self.cache_size:
                    # Eliminar el más antiguo (FIFO simple)
                    self.cache.pop(next(iter(self.cache)))
                self.cache[ruc_clean] = contribuyente
                
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_exitosas"] += 1
                
                print(f"✅ RUC {ruc_clean} encontrado ({elapsed*1000:.1f}ms)")
                return contribuyente
            else:
                elapsed = time.time() - start_time
                self.stats["tiempo_total"] += elapsed
                self.stats["consultas_fallidas"] += 1
                print(f"❌ RUC {ruc_clean} no encontrado ({elapsed*1000:.1f}ms)")
                return None
                
        except ValueError as e:
            print(f"⚠️  Error validando RUC: {e}")
            self.stats["consultas_fallidas"] += 1
            return None
        except Exception as e:
            print(f"❌ Error en búsqueda: {e}")
            self.stats["consultas_fallidas"] += 1
            return None
    
    def buscar_por_nombre(self, nombre: str, limit: int = 10) -> List[Contribuyente]:
        """Buscar por nombre o razón social"""
        start_time = time.time()
        
        try:
            # Limpiar y preparar término de búsqueda
            search_term = f"%{nombre.strip().upper()}%"
            
            query = """
            SELECT * FROM contribuyentes 
            WHERE UPPER(nombre_razon_social) LIKE ? 
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (search_term, limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row) for row in rows]
            elapsed = time.time() - start_time
            
            print(f"🔍 Búsqueda por nombre '{nombre}': {len(resultados)} resultados ({elapsed*1000:.1f}ms)")
            return resultados
            
        except Exception as e:
            print(f"❌ Error en búsqueda por nombre: {e}")
            return []
    
    def buscar_por_departamento(self, departamento: str, limit: int = 20) -> List[Contribuyente]:
        """Buscar por departamento (primeros 2 dígitos del ubigeo)"""
        try:
            if len(departamento) != 2 or not departamento.isdigit():
                print("⚠️  Código de departamento debe ser 2 dígitos")
                return []
            
            query = """
            SELECT * FROM contribuyentes 
            WHERE ubigeo LIKE ? || '%'
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (departamento, limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row) for row in rows]
            print(f"📍 {len(resultados)} contribuyentes en departamento {departamento}")
            return resultados
            
        except Exception as e:
            print(f"❌ Error en búsqueda por departamento: {e}")
            return []
    
    def buscar_por_estado(self, estado: str, limit: int = 20) -> List[Contribuyente]:
        """Buscar por estado del contribuyente"""
        try:
            query = """
            SELECT * FROM contribuyentes 
            WHERE estado_contribuyente = ? 
            ORDER BY nombre_razon_social
            LIMIT ?
            """
            
            self.cursor.execute(query, (estado.upper(), limit))
            rows = self.cursor.fetchall()
            
            resultados = [Contribuyente.from_db_row(row) for row in rows]
            print(f"🏷️  {len(resultados)} contribuyentes con estado '{estado}'")
            return resultados
            
        except Exception as e:
            print(f"❌ Error en búsqueda por estado: {e}")
            return []
    
    def validar_ruc(self, ruc: str) -> Dict[str, Any]:
        """Validar si un RUC existe y retornar información básica"""
        contribuyente = self.buscar_por_ruc(ruc)
        
        if contribuyente:
            return {
                "valido": True,
                "ruc": contribuyente.ruc,
                "nombre": contribuyente.nombre_razon_social,
                "estado": contribuyente.estado_contribuyente,
                "condicion": contribuyente.condicion_domicilio,
                "direccion": contribuyente.direccion_simple
            }
        else:
            return {
                "valido": False,
                "ruc": ruc,
                "mensaje": "RUC no encontrado en el padrón"
            }
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtener estadísticas del servicio"""
        # Calcular promedios
        avg_time = 0
        if self.stats["total_consultas"] > 0:
            avg_time = self.stats["tiempo_total"] / self.stats["total_consultas"] * 1000  # ms
        
        cache_hit_rate = 0
        total_cache = self.stats["cache_hits"] + self.stats["cache_misses"]
        if total_cache > 0:
            cache_hit_rate = self.stats["cache_hits"] / total_cache * 100
        
        return {
            "estadisticas": {
                "total_consultas": self.stats["total_consultas"],
                "consultas_exitosas": self.stats["consultas_exitosas"],
                "consultas_fallidas": self.stats["consultas_fallidas"],
                "tasa_exito": f"{(self.stats['consultas_exitosas']/self.stats['total_consultas']*100):.1f}%" if self.stats["total_consultas"] > 0 else "0%",
                "tiempo_promedio": f"{avg_time:.1f}ms",
                "cache_hits": self.stats["cache_hits"],
                "cache_misses": self.stats["cache_misses"],
                "tasa_cache_hit": f"{cache_hit_rate:.1f}%",
                "registros_en_cache": len(self.cache)
            },
            "base_datos": {
                "ruta": self.db_path,
                "total_registros": self._get_total_registros()
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def _get_total_registros(self) -> int:
        """Obtener total de registros en la base de datos"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            return self.cursor.fetchone()[0]
        except:
            return 0
    
    def exportar_resultado(self, ruc: str, formato: str = "json") -> Optional[str]:
        """Exportar resultado en diferentes formatos"""
        contribuyente = self.buscar_por_ruc(ruc)
        
        if not contribuyente:
            return None
        
        if formato.lower() == "json":
            return contribuyente.to_json()
        
        elif formato.lower() == "csv":
            data = contribuyente.to_dict()
            csv_lines = [f"{key},{value}" for key, value in data.items()]
            return "\n".join(csv_lines)
        
        elif formato.lower() == "texto":
            data = contribuyente.to_dict()
            text_lines = [f"{key}: {value}" for key, value in data.items()]
            return "\n".join(text_lines)
        
        else:
            return contribuyente.to_json()
    
    def close(self):
        """Cerrar conexión a la base de datos"""
        if self.conn:
            self.conn.close()
            print("🔒 Conexión a base de datos cerrada")

class RUCSearchCLI:
    """Interfaz de línea de comandos para búsqueda de RUC"""
    
    def __init__(self):
        self.service = RUCService()
        self.running = True
        
    def print_menu(self):
        """Mostrar menú de opciones"""
        print("\n" + "="*60)
        print("🔍 SISTEMA DE BÚSQUEDA DE RUC - PADRÓN SUNAT")
        print("="*60)
        print("1. Buscar por RUC")
        print("2. Buscar por nombre")
        print("3. Buscar por departamento")
        print("4. Buscar por estado")
        print("5. Validar RUC")
        print("6. Ver estadísticas")
        print("7. Exportar resultado")
        print("8. Salir")
        print("="*60)
    
    def buscar_por_ruc_interactive(self):
        """Interfaz interactiva para búsqueda por RUC"""
        print("\n" + "-"*40)
        print("BUSCAR POR RUC")
        print("-"*40)
        
        ruc = input("Ingrese RUC (11 dígitos): ").strip()
        
        if not ruc:
            print("⚠️  No se ingresó RUC")
            return
        
        print(f"\n🔎 Buscando RUC: {ruc}...")
        resultado = self.service.buscar_por_ruc(ruc)
        
        if resultado:
            print("\n" + "="*40)
            print("✅ RESULTADO ENCONTRADO")
            print("="*40)
            
            data = resultado.to_dict()
            for key, value in data.items():
                if value:  # Solo mostrar campos con valor
                    # Formatear nombre de campo
                    display_key = key.replace("_", " ").title()
                    print(f"{display_key:25}: {value}")
            
            print("="*40)
        else:
            print(f"\n❌ RUC {ruc} no encontrado")
    
    def buscar_por_nombre_interactive(self):
        """Interfaz interactiva para búsqueda por nombre"""
        print("\n" + "-"*40)
        print("BUSCAR POR NOMBRE")
        print("-"*40)
        
        nombre = input("Ingrese nombre o razón social: ").strip()
        
        if not nombre or len(nombre) < 3:
            print("⚠️  Ingrese al menos 3 caracteres")
            return
        
        try:
            limit = int(input("Límite de resultados (default 10): ") or "10")
        except:
            limit = 10
        
        print(f"\n🔍 Buscando: '{nombre}'...")
        resultados = self.service.buscar_por_nombre(nombre, limit)
        
        if resultados:
            print(f"\n📋 {len(resultados)} RESULTADOS ENCONTRADOS")
            print("-"*60)
            
            for i, contrib in enumerate(resultados, 1):
                print(f"\n{i}. RUC: {contrib.ruc}")
                print(f"   Nombre: {contrib.nombre_razon_social[:50]}{'...' if len(contrib.nombre_razon_social or '') > 50 else ''}")
                print(f"   Estado: {contrib.estado_contribuyente}")
                print(f"   Dirección: {contrib.direccion_simple[:50]}{'...' if len(contrib.direccion_simple) > 50 else ''}")
                print(f"   Ubigeo: {contrib.ubigeo}")
            
            print("-"*60)
            
            # Opción para ver detalles de un resultado
            if resultados:
                seleccion = input("\n¿Ver detalles de un RUC? (número o Enter para salir): ").strip()
                if seleccion.isdigit():
                    idx = int(seleccion) - 1
                    if 0 <= idx < len(resultados):
                        self._mostrar_detalles_completos(resultados[idx])
        else:
            print(f"\n❌ No se encontraron resultados para '{nombre}'")
    
    def buscar_por_departamento_interactive(self):
        """Interfaz interactiva para búsqueda por departamento"""
        print("\n" + "-"*40)
        print("BUSCAR POR DEPARTAMENTO")
        print("-"*40)
        print("Ejemplos: 15 (Lima), 07 (Callao), 01 (Amazonas)")
        
        depto = input("Ingrese código de departamento (2 dígitos): ").strip()
        
        if len(depto) != 2 or not depto.isdigit():
            print("⚠️  Código inválido. Debe ser 2 dígitos.")
            return
        
        try:
            limit = int(input("Límite de resultados (default 20): ") or "20")
        except:
            limit = 20
        
        print(f"\n📍 Buscando en departamento {depto}...")
        resultados = self.service.buscar_por_departamento(depto, limit)
        
        self._mostrar_lista_resultados(resultados, f"en departamento {depto}")
    
    def buscar_por_estado_interactive(self):
        """Interfaz interactiva para búsqueda por estado"""
        print("\n" + "-"*40)
        print("BUSCAR POR ESTADO")
        print("-"*40)
        print("Estados comunes: ACTIVO, HABIDO, NO HABIDO, BAJA")
        
        estado = input("Ingrese estado: ").strip().upper()
        
        if not estado:
            print("⚠️  No se ingresó estado")
            return
        
        try:
            limit = int(input("Límite de resultados (default 20): ") or "20")
        except:
            limit = 20
        
        print(f"\n🏷️  Buscando con estado '{estado}'...")
        resultados = self.service.buscar_por_estado(estado, limit)
        
        self._mostrar_lista_resultados(resultados, f"con estado '{estado}'")
    
    def validar_ruc_interactive(self):
        """Interfaz interactiva para validar RUC"""
        print("\n" + "-"*40)
        print("VALIDAR RUC")
        print("-"*40)
        
        ruc = input("Ingrese RUC a validar: ").strip()
        
        if not ruc:
            print("⚠️  No se ingresó RUC")
            return
        
        print(f"\n📋 Validando RUC: {ruc}...")
        resultado = self.service.validar_ruc(ruc)
        
        print("\n" + "="*40)
        print("📊 RESULTADO DE VALIDACIÓN")
        print("="*40)
        
        for key, value in resultado.items():
            display_key = key.replace("_", " ").title()
            print(f"{display_key:20}: {value}")
        
        print("="*40)
    
    def ver_estadisticas(self):
        """Mostrar estadísticas del sistema"""
        print("\n" + "="*40)
        print("📈 ESTADÍSTICAS DEL SISTEMA")
        print("="*40)
        
        stats = self.service.obtener_estadisticas()
        
        # Estadísticas generales
        print("\n📊 ESTADÍSTICAS DE BÚSQUEDA:")
        for key, value in stats["estadisticas"].items():
            display_key = key.replace("_", " ").title()
            print(f"{display_key:25}: {value}")
        
        # Base de datos
        print("\n🗄️  BASE DE DATOS:")
        for key, value in stats["base_datos"].items():
            display_key = key.replace("_", " ").title()
            print(f"{display_key:25}: {value}")
        
        print(f"\n🕒 Última actualización: {stats['timestamp']}")
        print("="*40)
    
    def exportar_resultado_interactive(self):
        """Interfaz para exportar resultado"""
        print("\n" + "-"*40)
        print("EXPORTAR RESULTADO")
        print("-"*40)
        
        ruc = input("Ingrese RUC a exportar: ").strip()
        
        if not ruc:
            print("⚠️  No se ingresó RUC")
            return
        
        print("\nFormatos disponibles:")
        print("1. JSON")
        print("2. CSV")
        print("3. Texto plano")
        
        opcion = input("Seleccione formato (1-3): ").strip()
        
        formatos = {"1": "json", "2": "csv", "3": "texto"}
        formato = formatos.get(opcion, "json")
        
        print(f"\n💾 Exportando RUC {ruc} en formato {formato.upper()}...")
        resultado = self.service.exportar_resultado(ruc, formato)
        
        if resultado:
            # Mostrar resultado
            print("\n" + "="*60)
            print(f"📄 RESULTADO EXPORTADO ({formato.upper()})")
            print("="*60)
            print(resultado[:500])  # Mostrar primeros 500 caracteres
            if len(resultado) > 500:
                print("...\n[Contenido truncado, archivo completo guardado]")
            print("="*60)
            
            # Guardar en archivo
            filename = f"ruc_{ruc}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{formato}"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(resultado)
            print(f"✅ Archivo guardado como: {filename}")
        else:
            print(f"❌ No se pudo exportar RUC {ruc}")
    
    def _mostrar_lista_resultados(self, resultados, titulo):
        """Mostrar lista de resultados"""
        if resultados:
            print(f"\n📋 {len(resultados)} RESULTADOS {titulo.upper()}")
            print("-"*60)
            
            for i, contrib in enumerate(resultados, 1):
                print(f"\n{i}. RUC: {contrib.ruc}")
                print(f"   Nombre: {contrib.nombre_razon_social[:60]}{'...' if len(contrib.nombre_razon_social or '') > 60 else ''}")
                print(f"   Estado: {contrib.estado_contribuyente}")
                print(f"   Dirección: {contrib.direccion_simple[:50]}{'...' if len(contrib.direccion_simple) > 50 else ''}")
            
            print("-"*60)
            
            # Opción para ver detalles
            if resultados:
                seleccion = input("\n¿Ver detalles de un RUC? (número o Enter para salir): ").strip()
                if seleccion.isdigit():
                    idx = int(seleccion) - 1
                    if 0 <= idx < len(resultados):
                        self._mostrar_detalles_completos(resultados[idx])
        else:
            print(f"\n❌ No se encontraron resultados {titulo}")
    
    def _mostrar_detalles_completos(self, contribuyente):
        """Mostrar detalles completos de un contribuyente"""
        print("\n" + "="*60)
        print("📄 DETALLES COMPLETOS")
        print("="*60)
        
        data = contribuyente.to_dict()
        for key, value in data.items():
            if value:  # Solo mostrar campos con valor
                display_key = key.replace("_", " ").title()
                print(f"{display_key:25}: {value}")
        
        print("="*60)
        
        # Opción para exportar
        exportar = input("\n¿Exportar estos datos? (s/n): ").strip().lower()
        if exportar == 's':
            self._exportar_desde_detalles(contribuyente)
    
    def _exportar_desde_detalles(self, contribuyente):
        """Exportar desde vista de detalles"""
        print("\nFormatos disponibles:")
        print("1. JSON")
        print("2. CSV")
        print("3. Texto plano")
        
        opcion = input("Seleccione formato (1-3): ").strip()
        
        formatos = {"1": "json", "2": "csv", "3": "texto"}
        formato = formatos.get(opcion, "json")
        
        resultado = contribuyente.to_json() if formato == "json" else self.service.exportar_resultado(contribuyente.ruc, formato)
        
        if resultado:
            filename = f"ruc_{contribuyente.ruc}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{formato}"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(resultado)
            print(f"✅ Archivo guardado como: {filename}")
    
    def run(self):
        """Ejecutar la interfaz CLI"""
        print("🚀 Iniciando sistema de búsqueda de RUC...")
        print(f"📁 Base de datos: {self.service.db_path}")
        
        try:
            while self.running:
                self.print_menu()
                
                try:
                    opcion = input("\nSeleccione una opción (1-8): ").strip()
                    
                    if opcion == "1":
                        self.buscar_por_ruc_interactive()
                    elif opcion == "2":
                        self.buscar_por_nombre_interactive()
                    elif opcion == "3":
                        self.buscar_por_departamento_interactive()
                    elif opcion == "4":
                        self.buscar_por_estado_interactive()
                    elif opcion == "5":
                        self.validar_ruc_interactive()
                    elif opcion == "6":
                        self.ver_estadisticas()
                    elif opcion == "7":
                        self.exportar_resultado_interactive()
                    elif opcion == "8":
                        print("\n👋 Saliendo del sistema...")
                        self.running = False
                    else:
                        print("⚠️  Opción inválida. Intente nuevamente.")
                    
                    # Pausa antes de mostrar menú nuevamente
                    if self.running:
                        input("\nPresione Enter para continuar...")
                        
                except KeyboardInterrupt:
                    print("\n\n⚠️  Interrupción detectada. Saliendo...")
                    self.running = False
                except Exception as e:
                    print(f"\n❌ Error: {e}")
                    input("Presione Enter para continuar...")
        
        finally:
            self.service.close()
            print("✅ Sistema finalizado correctamente.")

# Ejemplo de uso rápido
def ejemplo_rapido():
    """Ejemplo rápido de cómo usar el servicio"""
    print("🚀 EJEMPLO RÁPIDO DE USO")
    print("="*50)
    
    # Crear servicio
    servicio = RUCService()
    
    # Ejemplo 1: Buscar RUC específico
    print("\n1. Buscando RUC 10452159428...")
    resultado = servicio.buscar_por_ruc("10452159428")
    if resultado:
        print(f"   ✅ Encontrado: {resultado.nombre_razon_social}")
        print(f"   📍 Dirección: {resultado.direccion_simple}")
    else:
        print("   ❌ No encontrado")
    
    # Ejemplo 2: Validar RUC
    print("\n2. Validando RUC 20131312955...")
    validacion = servicio.validar_ruc("20131312955")
    print(f"   Válido: {validacion['valido']}")
    if validacion['valido']:
        print(f"   Nombre: {validacion['nombre']}")
    
    # Ejemplo 3: Estadísticas
    print("\n3. Estadísticas del servicio:")
    stats = servicio.obtener_estadisticas()
    print(f"   Consultas totales: {stats['estadisticas']['total_consultas']}")
    print(f"   Tasa de éxito: {stats['estadisticas']['tasa_exito']}")
    
    servicio.close()

# Punto de entrada principal
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Modo comando rápido
        if sys.argv[1] == "buscar":
            servicio = RUCService()
            if len(sys.argv) > 2:
                ruc = sys.argv[2]
                resultado = servicio.buscar_por_ruc(ruc)
                if resultado:
                    print(resultado.to_json())
                else:
                    print(f"RUC {ruc} no encontrado")
            servicio.close()
        
        elif sys.argv[1] == "ejemplo":
            ejemplo_rapido()
        
        elif sys.argv[1] == "consola":
            # Modo consola interactiva
            cli = RUCSearchCLI()
            cli.run()
        
        else:
            print("Uso: python ruc_search.py [comando]")
            print("Comandos disponibles:")
            print("  buscar <ruc>     - Buscar un RUC específico")
            print("  ejemplo          - Ejecutar ejemplo rápido")
            print("  consola          - Modo consola interactiva")
            print("\nEjemplo: python ruc_search.py buscar 10452159428")
    
    else:
        # Si no hay argumentos, iniciar consola interactiva
        cli = RUCSearchCLI()
        cli.run()