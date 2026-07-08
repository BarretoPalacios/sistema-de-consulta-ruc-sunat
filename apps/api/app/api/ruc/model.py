from typing import Optional
from pydantic import BaseModel


class ContribuyenteOut(BaseModel):
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

    @classmethod
    def from_row(cls, row) -> "ContribuyenteOut":
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
