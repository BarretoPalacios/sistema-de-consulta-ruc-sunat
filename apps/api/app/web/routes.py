import csv
import io
import json
import os

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from apps.api.app.api.ruc.service import RUCService
from packages.config.settings import settings

templates = Jinja2Templates(directory="apps/api/app/web/templates", cache_size=0)

router = APIRouter()
ruc_service = RUCService()


def _get_updater_status() -> dict:
    metadata_path = os.path.join(os.path.dirname(settings.DATABASE_URL) or "data", "metadata.json")
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "title": "Sistema de Búsqueda de RUC",
        "page": "home",
    })


@router.post("/buscar", response_class=HTMLResponse)
async def buscar_ruc(request: Request, ruc: str = Form(...)):
    return templates.TemplateResponse("detalle.html", {
        "request": request,
        "title": "Resultado de Búsqueda",
        "page": "detalle",
        "result": ruc_service.buscar_por_ruc(ruc, source="web"),
        "ruc": ruc,
    })


@router.get("/detalle/{ruc}", response_class=HTMLResponse)
async def detalle_ruc(request: Request, ruc: str):
    return templates.TemplateResponse("detalle.html", {
        "request": request,
        "title": "Resultado de Búsqueda",
        "page": "detalle",
        "result": ruc_service.buscar_por_ruc(ruc, source="web"),
        "ruc": ruc,
    })


@router.get("/docs", response_class=HTMLResponse)
async def docs_page(request: Request):
    return templates.TemplateResponse("docs.html", {
        "request": request,
        "title": "Documentación de la API",
        "page": "docs",
    })


@router.get("/estadisticas", response_class=HTMLResponse)
async def estadisticas_page(request: Request):
    stats = ruc_service.obtener_estadisticas()
    updater_info = _get_updater_status()
    return templates.TemplateResponse("estadisticas.html", {
        "request": request,
        "title": "Estadísticas del Sistema",
        "page": "estadisticas",
        "stats": stats,
        "updater": updater_info,
    })


@router.post("/exportar/descargar")
async def exportar_descargar(ruc: str = Form(...), formato: str = Form("json")):
    result = ruc_service.buscar_por_ruc(ruc, source="web")
    if not result["success"]:
        return JSONResponse(content={"error": result["error"]}, status_code=404)

    data = result["data"]

    if formato == "json":
        content = json.dumps(data, ensure_ascii=False, indent=2)
        filename = f"ruc_{ruc}.json"
        media_type = "application/json"
    elif formato == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(data.keys())
        writer.writerow(data.values())
        content = output.getvalue()
        filename = f"ruc_{ruc}.csv"
        media_type = "text/csv"
    elif formato == "texto":
        lines = [f"{key}: {value}" for key, value in data.items()]
        content = "\n".join(lines)
        filename = f"ruc_{ruc}.txt"
        media_type = "text/plain"
    else:
        return JSONResponse(content={"error": "Formato no soportado"}, status_code=400)

    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
