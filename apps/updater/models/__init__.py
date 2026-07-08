from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Contribuyente:
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

    @classmethod
    def from_row(cls, row) -> "Contribuyente":
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
            kilometro=row[14],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ruc": self.ruc,
            "nombre_razon_social": self.nombre_razon_social,
            "estado_contribuyente": self.estado_contribuyente,
            "condicion_domicilio": self.condicion_domicilio,
            "ubigeo": self.ubigeo,
            "tipo_via": self.tipo_via,
            "nombre_via": self.nombre_via,
            "codigo_zona": self.codigo_zona,
            "tipo_zona": self.tipo_zona,
            "numero": self.numero,
            "interior": self.interior,
            "lote": self.lote,
            "departamento": self.departamento,
            "manzana": self.manzana,
            "kilometro": self.kilometro,
        }
