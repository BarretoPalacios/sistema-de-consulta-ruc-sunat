from dataclasses import dataclass
from typing import Optional, Dict, Any
import json
from pydantic import BaseModel

class ContribuyenteModel(BaseModel):
    """Modelo Pydantic para contribuyentes"""
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

class SearchRequest(BaseModel):
    """Modelo para solicitud de búsqueda"""
    query: str
    limit: Optional[int] = 10

class ExportRequest(BaseModel):
    """Modelo para solicitud de exportación"""
    ruc: str
    format: str = "json"