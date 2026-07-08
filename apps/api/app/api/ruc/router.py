from fastapi import APIRouter, HTTPException, Request, Depends

from apps.api.app.core.limiter import limiter
from apps.api.app.core.config import api_settings
from apps.api.app.core.security import verify_token
from apps.api.app.api.ruc.service import RUCService
from apps.api.app.api.ruc.schema import ExportRequest

router = APIRouter()
ruc_service = RUCService()


@router.get("/ruc/{ruc}")
@limiter.limit(f"{api_settings.RATE_LIMIT_PER_MINUTE}/minute")
async def buscar_ruc(request: Request, ruc: str, token: str = Depends(verify_token)):
    result = ruc_service.buscar_por_ruc(ruc, source="api")
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/stats")
@limiter.limit(f"{api_settings.RATE_LIMIT_PER_MINUTE}/minute")
async def obtener_estadisticas(request: Request, token: str = Depends(verify_token)):
    result = ruc_service.obtener_estadisticas()
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@router.get("/status")
async def get_status(token: str = Depends(verify_token)):
    return ruc_service.obtener_status()


@router.post("/export")
@limiter.limit(f"{api_settings.RATE_LIMIT_PER_MINUTE}/minute")
async def exportar_datos(request: Request, export_req: ExportRequest, token: str = Depends(verify_token)):
    result = ruc_service.exportar(export_req.ruc, export_req.format)
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
