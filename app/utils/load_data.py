import sqlite3
import time
import psutil
import sys
import os
from datetime import datetime

class TxtToSQLiteConverter:
    def __init__(self, txt_file, db_file, batch_size=30000):
        """
        Inicializar el conversor optimizado para 4GB RAM
        
        Args:
            txt_file: Ruta del archivo TXT de entrada
            db_file: Ruta de la base de datos SQLite de salida
            batch_size: Tamaño del lote (ajustado para 4GB RAM)
        """
        self.txt_file = txt_file
        self.db_file = db_file
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.total_records = 0
        self.start_time = None
        self.encoding = None
        
    def detect_encoding(self):
        """Detectar la codificación del archivo"""
        print("Detectando codificación del archivo...")
        
        # Codificaciones comunes para archivos SUNAT
        encodings_to_try = ['latin-1', 'iso-8859-1', 'cp1252', 'utf-8']
        
        for encoding in encodings_to_try:
            try:
                with open(self.txt_file, 'r', encoding=encoding) as f:
                    # Leer primeras líneas para probar
                    for _ in range(5):
                        line = f.readline()
                        if '|' in line:  # Verificar que tenga el formato esperado
                            self.encoding = encoding
                            print(f"✓ Codificación detectada: {encoding}")
                            return encoding
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue
        
        # Si no detecta, usar latin-1 por defecto (común en SUNAT)
        self.encoding = 'latin-1'
        print(f"⚠ Usando codificación por defecto: {self.encoding}")
        return self.encoding
    
    def check_memory(self):
        """Verificar memoria disponible"""
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        
        print(f"Memoria disponible: {available_gb:.2f} GB")
        print(f"Memoria total: {memory.total / (1024**3):.2f} GB")
        
        if available_gb < 1:
            print("¡ADVERTENCIA! Memoria disponible baja. Reduciendo batch_size...")
            self.batch_size = 15000
            
        return available_gb
    
    def connect_db(self):
        """Conectar a la base de datos SQLite con optimizaciones"""
        print("Conectando a la base de datos...")
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()
        
        # Optimizaciones para inserción masiva
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA cache_size = -2000")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute("PRAGMA mmap_size = 268435456")
        self.conn.execute("PRAGMA page_size = 4096")
        
        print("Conexión establecida con optimizaciones activadas")
    
    def create_table(self):
        """Crear la tabla con los campos correspondientes"""
        print("Creando tabla en la base de datos...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS contribuyentes (
            ruc TEXT PRIMARY KEY,
            nombre_razon_social TEXT,
            estado_contribuyente TEXT,
            condicion_domicilio TEXT,
            ubigeo TEXT,
            tipo_via TEXT,
            nombre_via TEXT,
            codigo_zona TEXT,
            tipo_zona TEXT,
            numero TEXT,
            interior TEXT,
            lote TEXT,
            departamento TEXT,
            manzana TEXT,
            kilometro TEXT
        );
        """
        
        try:
            self.cursor.execute("DROP TABLE IF EXISTS contribuyentes")
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            print("Tabla 'contribuyentes' creada exitosamente")
        except Exception as e:
            print(f"Error al crear tabla: {e}")
            raise
    
    def clean_value(self, value):
        """Limpiar y convertir valores"""
        if not value or value == '-' or value.strip() == '':
            return None
        
        # Limpiar espacios y caracteres extraños
        cleaned = value.strip()
        
        # Reemplazar caracteres problemáticos comunes
        replacements = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã±': 'ñ', 'Ã': 'í', 'Ã': 'Á', 'Ã': 'É', 'Ã': 'Í',
            'Ã': 'Ó', 'Ã': 'Ú', 'Ã': 'Ñ', 'Â': '', 'â€“': '-',
            'â€œ': '"', 'â€': '"', 'â€¢': '-', 'â€¦': '...'
        }
        
        for wrong, correct in replacements.items():
            cleaned = cleaned.replace(wrong, correct)
        
        return cleaned
    
    def parse_line(self, line):
        """Parsear una línea del archivo"""
        try:
            # Limpiar la línea
            cleaned = line.strip()
            if not cleaned:
                return None
            
            # Dividir por pipe
            values = cleaned.split('|')
            
            # Asegurar que tenemos 15 campos
            if len(values) < 15:
                # Rellenar con None si faltan campos
                values.extend([None] * (15 - len(values)))
            elif len(values) > 15:
                # Si hay más campos, unir los campos extra en el nombre (campo 1)
                extra_fields = values[15:]
                values = values[:15]
                if extra_fields and values[1]:  # Si hay nombre
                    values[1] = values[1] + ' ' + ' '.join(extra_fields)
            
            # Limpiar cada valor
            cleaned_values = []
            for v in values:
                if v is not None:
                    cleaned_v = self.clean_value(v)
                    cleaned_values.append(cleaned_v)
                else:
                    cleaned_values.append(None)
            
            return cleaned_values
            
        except Exception as e:
            print(f"Error al parsear línea: {str(e)[:100]}")
            return None
    
    def insert_batch(self, batch):
        """Insertar un lote de registros de manera eficiente"""
        if not batch:
            return
            
        placeholders = ','.join(['?'] * 15)
        sql = f"INSERT OR REPLACE INTO contribuyentes VALUES ({placeholders})"
        
        try:
            self.cursor.executemany(sql, batch)
        except Exception as e:
            # Si falla el batch, insertar uno por uno
            print(f"Error en batch insert, insertando uno por uno...")
            success_count = 0
            for record in batch:
                try:
                    self.cursor.execute(
                        "INSERT OR REPLACE INTO contribuyentes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        record
                    )
                    success_count += 1
                except Exception as e2:
                    continue
            print(f"Insertados {success_count}/{len(batch)} registros del batch")
    
    def load_data(self):
        """Cargar datos desde el archivo TXT en lotes"""
        print(f"\nIniciando carga de datos (codificación: {self.encoding})...")
        print(f"Tamaño de lote: {self.batch_size:,} registros")
        
        self.start_time = time.time()
        batch_records = []
        lines_processed = 0
        batch_count = 0
        error_count = 0
        
        try:
            # Iniciar transacción
            self.cursor.execute("BEGIN TRANSACTION")
            
            with open(self.txt_file, 'r', encoding=self.encoding, errors='replace') as file:
                # Leer encabezado
                header = file.readline()
                print(f"Encabezado: {header[:100]}...")
                
                for line_num, line in enumerate(file, 1):
                    try:
                        # Parsear la línea
                        record = self.parse_line(line)
                        if record:
                            batch_records.append(record)
                        else:
                            error_count += 1
                        
                        # Insertar cuando el lote esté lleno
                        if len(batch_records) >= self.batch_size:
                            self.insert_batch(batch_records)
                            self.total_records += len(batch_records)
                            batch_count += 1
                            
                            # Mostrar progreso
                            if batch_count % 10 == 0:
                                self.show_progress(line_num, error_count)
                            
                            # Liberar memoria
                            batch_records = []
                            
                            # Pequeña pausa para no saturar
                            if batch_count % 100 == 0:
                                time.sleep(0.01)
                                import gc
                                gc.collect()
                        
                        lines_processed += 1
                        
                        # Mostrar progreso cada 100,000 líneas
                        if lines_processed % 100000 == 0:
                            self.show_progress(lines_processed, error_count)
                        
                    except Exception as e:
                        error_count += 1
                        if lines_processed % 100000 == 0:
                            print(f"Error en línea {line_num}: {str(e)[:100]}")
                        continue
            
            # Insertar los registros restantes
            if batch_records:
                self.insert_batch(batch_records)
                self.total_records += len(batch_records)
            
            # Finalizar transacción
            self.conn.commit()
            
            # Mostrar resultados finales
            self.show_final_results(lines_processed, error_count)
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error durante la carga: {e}")
            raise
        finally:
            # Forzar liberación de memoria
            import gc
            gc.collect()
    
    def show_progress(self, current_line, error_count):
        """Mostrar progreso de la carga"""
        elapsed = time.time() - self.start_time
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        
        # Calcular velocidad
        lines_per_second = current_line / elapsed if elapsed > 0 else 0
        
        # Estimar tiempo restante (asumiendo ~18M líneas)
        estimated_total = 18000000
        if lines_per_second > 0 and current_line > 100000:
            remaining_seconds = (estimated_total - current_line) / lines_per_second
            rem_hours, rem_rem = divmod(remaining_seconds, 3600)
            rem_minutes, rem_seconds = divmod(rem_rem, 60)
            eta = f"{int(rem_hours):02d}:{int(rem_minutes):02d}:{int(rem_seconds):02d}"
        else:
            eta = "calculando..."
        
        # Mostrar estadísticas
        print(f"\n{'='*60}")
        print(f"Líneas procesadas: {current_line:,} / ~18,000,000")
        print(f"Registros insertados: {self.total_records:,}")
        print(f"Errores: {error_count:,}")
        print(f"Tiempo: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print(f"ETA: {eta}")
        print(f"Velocidad: {lines_per_second:.0f} líneas/seg")
        
        # Mostrar uso de memoria
        memory = psutil.virtual_memory()
        print(f"Memoria: {memory.percent}% usado ({memory.available/(1024**3):.1f} GB libre)")
        print(f"{'='*60}")
    
    def show_final_results(self, total_lines, error_count):
        """Mostrar resultados finales"""
        elapsed = time.time() - self.start_time
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        
        print(f"\n{'='*60}")
        print("CARGA COMPLETADA EXITOSAMENTE!")
        print(f"{'='*60}")
        print(f"Líneas totales en archivo: {total_lines:,}")
        print(f"Registros insertados en BD: {self.total_records:,}")
        print(f"Líneas con errores: {error_count:,}")
        print(f"Tasa de éxito: {(self.total_records/total_lines*100):.2f}%")
        print(f"Tiempo total: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print(f"Velocidad promedio: {total_lines/elapsed:.0f} líneas/segundo")
        print(f"Registros/segundo: {self.total_records/elapsed:.0f}")
        
        # Verificar conteo en base de datos
        self.cursor.execute("SELECT COUNT(*) FROM contribuyentes")
        db_count = self.cursor.fetchone()[0]
        print(f"Registros en base de datos: {db_count:,}")
        
        if self.total_records != db_count:
            print(f"ADVERTENCIA: Diferencia: {abs(self.total_records - db_count):,}")
    
    def create_indexes(self):
        """Crear índices después de la carga"""
        print("\nCreando índices...")
        idx_start = time.time()
        
        # Desactivar journal temporalmente para índices más rápidos
        self.conn.execute("PRAGMA journal_mode = OFF")
        
        indexes = [
            "CREATE INDEX idx_estado ON contribuyentes(estado_contribuyente)",
            "CREATE INDEX idx_ubigeo ON contribuyentes(ubigeo)",
            "CREATE INDEX idx_nombre ON contribuyentes(nombre_razon_social)",
            "CREATE INDEX idx_condicion ON contribuyentes(condicion_domicilio)",
        ]
        
        for i, idx_sql in enumerate(indexes, 1):
            try:
                print(f"Creando índice {i}/{len(indexes)}...")
                idx_sql_start = time.time()
                self.cursor.execute(idx_sql)
                idx_sql_elapsed = time.time() - idx_sql_start
                print(f"  Índice creado en {idx_sql_elapsed:.1f} segundos")
            except Exception as e:
                print(f"  Error creando índice: {e}")
        
        self.conn.commit()
        
        # Reactivar journal
        self.conn.execute("PRAGMA journal_mode = WAL")
        
        idx_elapsed = time.time() - idx_start
        print(f"Índices creados en {idx_elapsed:.1f} segundos")
    
    def optimize_database(self):
        """Optimizar la base de datos"""
        print("\nOptimizando base de datos...")
        opt_start = time.time()
        
        try:
            # Vacuum para optimizar espacio
            print("Ejecutando VACUUM...")
            self.cursor.execute("VACUUM")
            
            # Analizar para optimizar consultas
            print("Ejecutando ANALYZE...")
            self.cursor.execute("ANALYZE")
            
            self.conn.commit()
            
            opt_elapsed = time.time() - opt_start
            print(f"Base de datos optimizada en {opt_elapsed:.1f} segundos")
            
        except Exception as e:
            print(f"Error durante optimización: {e}")
    
    def verify_data(self):
        """Verificar datos cargados"""
        print("\nVerificando datos...")
        
        try:
            # Conteo total
            self.cursor.execute("SELECT COUNT(*) FROM contribuyentes")
            total = self.cursor.fetchone()[0]
            print(f"Total registros en BD: {total:,}")
            
            # Conteo por estado
            self.cursor.execute("""
                SELECT estado_contribuyente, COUNT(*) 
                FROM contribuyentes 
                GROUP BY estado_contribuyente
                ORDER BY COUNT(*) DESC
            """)
            print("\nRegistros por estado:")
            for estado, count in self.cursor.fetchall():
                print(f"  {estado or 'NULL'}: {count:,}")
            
            # Mostrar algunos registros de muestra
            print("\nMuestra de 3 registros:")
            self.cursor.execute("SELECT * FROM contribuyentes LIMIT 3")
            columns = [desc[0] for desc in self.cursor.description]
            
            for row in self.cursor.fetchall():
                print("-" * 40)
                for col, val in zip(columns, row):
                    if val:
                        display_val = str(val)
                        if len(display_val) > 50:
                            display_val = display_val[:50] + "..."
                        print(f"{col}: {display_val}")
                    else:
                        print(f"{col}: NULL")
            
        except Exception as e:
            print(f"Error en verificación: {e}")
    
    def close(self):
        """Cerrar conexiones"""
        if self.conn:
            self.conn.close()
            print("\nConexión a base de datos cerrada")


def main():
    """Función principal"""
    print("=" * 70)
    print("CONVERSOR DE PADRÓN SUNAT A SQLITE - OPTIMIZADO PARA 4GB RAM")
    print("=" * 70)
    
    # Configuración
    TXT_FILE = "padron.txt"
    DB_FILE = "contribuyentes.db"
    
    # Para 4GB RAM
    BATCH_SIZE = 30000
    
    print(f"\nConfiguración:")
    print(f"Archivo TXT: {TXT_FILE}")
    print(f"Base de datos: {DB_FILE}")
    print(f"Tamaño de lote: {BATCH_SIZE:,} registros")
    
    # Verificar que el archivo existe
    if not os.path.exists(TXT_FILE):
        print(f"\n❌ ERROR: No se encuentra el archivo {TXT_FILE}")
        print(f"Buscar en: {os.path.abspath('.')}")
        
        # Listar archivos en el directorio
        print("\nArchivos en el directorio actual:")
        for file in os.listdir('.'):
            if file.endswith('.txt'):
                print(f"  - {file}")
        
        return
    
    # Verificar tamaño del archivo
    file_size = os.path.getsize(TXT_FILE) / (1024**3)  # GB
    print(f"Tamaño del archivo: {file_size:.2f} GB")
    
    # Crear instancia del conversor
    converter = TxtToSQLiteConverter(TXT_FILE, DB_FILE, BATCH_SIZE)
    
    # Detectar codificación
    encoding = converter.detect_encoding()
    
    # Verificar memoria
    available_memory = converter.check_memory()
    
    if available_memory < 0.5:
        print("\n¡ERROR! Memoria insuficiente (menos de 0.5GB disponible).")
        print("Por favor, cierra otras aplicaciones antes de continuar.")
        return
    
    # Preguntar confirmación
    print("\n" + "=" * 70)
    print("ESTIMACIÓN:")
    print(f"- Tamaño archivo: {file_size:.2f} GB")
    print(f"- Registros estimados: ~18,000,000")
    print(f"- Tiempo estimado: 3-6 horas")
    print(f"- Espacio BD final: ~2-3 GB")
    print("=" * 70)
    
    respuesta = input("\n¿Deseas continuar con la conversión? (s/n): ").lower()
    
    if respuesta != 's':
        print("Proceso cancelado por el usuario.")
        return
    
    try:
        # Paso 1: Conectar y crear tabla
        print("\n" + "=" * 70)
        print("PASO 1: Configurando base de datos")
        print("=" * 70)
        converter.connect_db()
        converter.create_table()
        
        # Paso 2: Cargar datos
        print("\n" + "=" * 70)
        print("PASO 2: Cargando datos (esto puede tomar varias horas)")
        print("=" * 70)
        print("Puedes presionar Ctrl+C para interrumpir si es necesario")
        print("=" * 70)
        converter.load_data()
        
        # Paso 3: Crear índices
        print("\n" + "=" * 70)
        print("PASO 3: Creando índices")
        print("=" * 70)
        converter.create_indexes()
        
        # Paso 4: Optimizar
        print("\n" + "=" * 70)
        print("PASO 4: Optimizando base de datos")
        print("=" * 70)
        converter.optimize_database()
        
        # Paso 5: Verificar
        print("\n" + "=" * 70)
        print("PASO 5: Verificando datos")
        print("=" * 70)
        converter.verify_data()
        
        print("\n" + "=" * 70)
        print("¡PROCESO COMPLETADO EXITOSAMENTE!")
        print("=" * 70)
        print(f"Base de datos creada: {DB_FILE}")
        print("Puedes usar herramientas como DB Browser for SQLite")
        print("para explorar los datos: https://sqlitebrowser.org/")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n✋ Proceso interrumpido por el usuario.")
        print("La base de datos puede estar incompleta.")
        print("Elimina el archivo .db y comienza de nuevo.")
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
    finally:
        converter.close()
        
        # Mostrar uso final de memoria
        memory = psutil.virtual_memory()
        print(f"\nMemoria usada al final: {memory.percent}%")
        print(f"Memoria disponible: {memory.available / (1024**3):.2f} GB")


if __name__ == "__main__":
    # Instalar psutil si no está instalado
    try:
        import psutil
    except ImportError:
        print("Instalando psutil para monitoreo de memoria...")
        try:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
            import psutil
            print("✓ psutil instalado correctamente")
        except:
            print("⚠ No se pudo instalar psutil. Continuando sin monitoreo de memoria...")
            # Crear un mock de psutil
            class MockPsutil:
                class virtual_memory:
                    @staticmethod
                    def available():
                        return 4 * 1024**3  # 4 GB
                    @staticmethod
                    def total():
                        return 8 * 1024**3  # 8 GB
                    @staticmethod
                    def percent():
                        return 50
            psutil = MockPsutil()
    
    # Ejecutar programa
    main()