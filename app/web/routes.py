from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import io
import csv
import json

from app.services.ruc_service import RUCService

# Configurar templates
templates = Jinja2Templates(directory="app/web/templates")

router = APIRouter()
ruc_service = RUCService()

@router.get("/buscar", response_class=HTMLResponse)
async def buscar_page(request: Request):
    """Página de búsqueda por RUC"""
    return templates.TemplateResponse("buscar_ruc.html", {
        "request": request,
        "title": "Buscar por RUC",
        "page": "buscar"
    })

@router.post("/buscar/resultado")
async def buscar_resultado(request: Request, ruc: str = Form(...)):
    """Procesar búsqueda por RUC"""
    result = ruc_service.buscar_por_ruc(ruc, source="web")
    
    if result["success"]:
        return templates.TemplateResponse("resultados.html", {
            "request": request,
            "title": "Resultado de Búsqueda",
            "page": "resultado",
            "result": result,
            "success": True
        })
    else:
        return templates.TemplateResponse("resultados.html", {
            "request": request,
            "title": "Error en Búsqueda",
            "page": "error",
            "error": result["error"],
            "ruc": ruc,
            "success": False
        })

@router.get("/buscar-nombre", response_class=HTMLResponse)
async def buscar_nombre_page(request: Request):
    """Página de búsqueda por nombre"""
    return templates.TemplateResponse("buscar_nombre.html", {
        "request": request,
        "title": "Buscar por Nombre",
        "page": "buscar_nombre"
    })

@router.post("/buscar-nombre/resultado")
async def buscar_nombre_resultado(request: Request, nombre: str = Form(...), limit: int = Form(10)):
    """Procesar búsqueda por nombre"""
    result = ruc_service.buscar_por_nombre(nombre, limit)
    
    return templates.TemplateResponse("resultados_nombre.html", {
        "request": request,
        "title": "Resultados por Nombre",
        "page": "resultados_nombre",
        "result": result,
        "query": nombre,
        "limit": limit
    })

@router.get("/buscar-departamento", response_class=HTMLResponse)
async def buscar_departamento_page(request: Request):
    """Página de búsqueda por departamento"""
    deptos_result = ruc_service.obtener_departamentos()
    departamentos = deptos_result["data"] if deptos_result["success"] else []
    
    return templates.TemplateResponse("buscar_departamento.html", {
        "request": request,
        "title": "Buscar por Departamento",
        "page": "buscar_departamento",
        "departamentos": departamentos
    })

@router.post("/buscar-departamento/resultado")
async def buscar_departamento_resultado(request: Request, departamento: str = Form(...), limit: int = Form(20)):
    """Procesar búsqueda por departamento"""
    result = ruc_service.buscar_por_departamento(departamento, limit)
    
    return templates.TemplateResponse("resultados_departamento.html", {
        "request": request,
        "title": "Resultados por Departamento",
        "page": "resultados_departamento",
        "result": result,
        "departamento": departamento,
        "limit": limit
    })

@router.get("/buscar-estado", response_class=HTMLResponse)
async def buscar_estado_page(request: Request):
    """Página de búsqueda por estado"""
    estados_result = ruc_service.obtener_estados()
    estados = estados_result["data"] if estados_result["success"] else []
    
    return templates.TemplateResponse("buscar_estado.html", {
        "request": request,
        "title": "Buscar por Estado",
        "page": "buscar_estado",
        "estados": estados
    })

@router.post("/buscar-estado/resultado")
async def buscar_estado_resultado(request: Request, estado: str = Form(...), limit: int = Form(20)):
    """Procesar búsqueda por estado"""
    result = ruc_service.buscar_por_estado(estado, limit)
    
    return templates.TemplateResponse("resultados_estado.html", {
        "request": request,
        "title": "Resultados por Estado",
        "page": "resultados_estado",
        "result": result,
        "estado": estado,
        "limit": limit
    })

@router.get("/estadisticas", response_class=HTMLResponse)
async def estadisticas_page(request: Request):
    """Página de estadísticas"""
    result = ruc_service.obtener_estadisticas()
    
    return templates.TemplateResponse("estadisticas.html", {
        "request": request,
        "title": "Estadísticas del Sistema",
        "page": "estadisticas",
        "result": result
    })

@router.get("/exportar", response_class=HTMLResponse)
async def exportar_page(request: Request):
    """Página de exportación"""
    return templates.TemplateResponse("exportar.html", {
        "request": request,
        "title": "Exportar Datos",
        "page": "exportar"
    })

@router.post("/exportar/descargar")
async def exportar_descargar(ruc: str = Form(...), formato: str = Form("json")):
    """Descargar datos exportados"""
    result = ruc_service.buscar_por_ruc(ruc, source="web")
    
    if not result["success"]:
        return JSONResponse(
            content={"error": result["error"]},
            status_code=404
        )
    
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
        return JSONResponse(
            content={"error": "Formato no soportado"},
            status_code=400
        )
    
    return StreamingResponse(
        io.BytesIO(content.encode('utf-8')),
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )