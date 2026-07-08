from pydantic import BaseModel


class ExportRequest(BaseModel):
    ruc: str
    format: str = "json"
