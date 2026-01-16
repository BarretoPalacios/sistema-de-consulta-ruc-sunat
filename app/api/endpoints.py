from fastapi import APIRouter, HTTPException, Query, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional

from app.config import settings
from app.services.ruc_service import RUCService
from app.models.contribuyente import SearchRequest, ExportRequest

# Configurar seguridad
security = HTTPBearer()

# Función para verificar el token
async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    """
    Verifica si el token Bearer es válido usando la configuración
    """
    if credentials.credentials != settings.API_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Token de acceso inválido o faltante",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials

router = APIRouter()
ruc_service = RUCService()

@router.get("/")
async def api_root():
    """Raíz de la API (público)"""
    return {
        "service": settings.API_TITLE,
        "version": settings.API_VERSION,
        "description": settings.API_DESCRIPTION,
        "authentication": "Bearer Token requerido para endpoints protegidos",
        "security_notice": "Esta API requiere token de autenticación",
        "endpoints": [
            {"path": "/api/v1/buscar/{ruc}", "method": "GET", "description": "Buscar contribuyente por RUC", "auth": True},
            {"path": "/api/v1/buscar/nombre/{nombre}", "method": "GET", "description": "Buscar por nombre o razón social", "auth": True},
            {"path": "/api/v1/buscar/departamento/{departamento}", "method": "GET", "description": "Buscar por departamento", "auth": True},
            {"path": "/api/v1/buscar/estado/{estado}", "method": "GET", "description": "Buscar por estado", "auth": True},
            {"path": "/api/v1/estadisticas", "method": "GET", "description": "Obtener estadísticas del sistema", "auth": True},
            {"path": "/api/v1/departamentos", "method": "GET", "description": "Listar departamentos disponibles", "auth": True},
            {"path": "/api/v1/estados", "method": "GET", "description": "Listar estados disponibles", "auth": True},
            {"path": "/api/v1/exportar", "method": "POST", "description": "Exportar datos en diferentes formatos", "auth": True}
        ],
        "rate_limit": f"{settings.RATE_LIMIT_PER_MINUTE} solicitudes por minuto",
        "cache_ttl": f"{settings.CACHE_TTL} segundos"
    }

@router.get("/buscar/{ruc}")
async def buscar_ruc(
    ruc: str,
    token: str = Depends(verify_token)
):
    """
    Buscar contribuyente por RUC
    
    - **ruc**: RUC de 11 dígitos del contribuyente
    - Retorna: Información completa del contribuyente incluyendo nombre, estado, dirección, etc.
    """
    result = ruc_service.buscar_por_ruc(ruc, source="api")
    
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["error"])
    
    return result

# @router.get("/buscar/nombre/{nombre}")
# async def buscar_nombre(
#     nombre: str,
#     limit: int = Query(10, ge=1, le=100, description="Límite de resultados (1-100)"),
#     token: str = Depends(verify_token)
# ):
#     """
#     Buscar contribuyentes por nombre o razón social
    
#     - **nombre**: Nombre o razón social (mínimo 3 caracteres)
#     - **limit**: Límite de resultados (1-100)
#     - Retorna: Lista de contribuyentes que coinciden con la búsqueda
#     """
#     result = ruc_service.buscar_por_nombre(nombre, limit)
    
#     if not result["success"]:
#         raise HTTPException(status_code=400, detail=result["error"])
    
#     return result

# @router.get("/buscar/departamento/{departamento}")
# async def buscar_departamento(
#     departamento: str,
#     limit: int = Query(20, ge=1, le=100, description="Límite de resultados (1-100)"),
#     token: str = Depends(verify_token)
# ):
#     """
#     Buscar contribuyentes por departamento
    
#     - **departamento**: Código de departamento (2 dígitos) o nombre
#     - **limit**: Límite de resultados (1-100)
#     - Retorna: Lista de contribuyentes en el departamento especificado
#     """
#     result = ruc_service.buscar_por_departamento(departamento, limit)
    
#     if not result["success"]:
#         raise HTTPException(status_code=400, detail=result["error"])
    
#     return result

# @router.get("/buscar/estado/{estado}")
# async def buscar_estado(
#     estado: str,
#     limit: int = Query(20, ge=1, le=100, description="Límite de resultados (1-100)"),
#     token: str = Depends(verify_token)
# ):
#     """
#     Buscar contribuyentes por estado
    
#     - **estado**: Estado del contribuyente (ACTIVO, HABIDO, BAJA, etc.)
#     - **limit**: Límite de resultados (1-100)
#     - Retorna: Lista de contribuyentes con el estado especificado
#     """
#     result = ruc_service.buscar_por_estado(estado, limit)
    
#     if not result["success"]:
#         raise HTTPException(status_code=400, detail=result["error"])
    
#     return result

@router.get("/estadisticas")
async def obtener_estadisticas(token: str = Depends(verify_token)):
    """
    Obtener estadísticas del sistema
    
    - Retorna: Estadísticas de uso, cache, y datos generales del sistema
    """
    result = ruc_service.obtener_estadisticas()
    
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    
    return result

# @router.get("/departamentos")
# async def obtener_departamentos(token: str = Depends(verify_token)):
#     """
#     Obtener lista de departamentos disponibles
    
#     - Retorna: Lista de todos los departamentos con códigos y nombres
#     """
#     result = ruc_service.obtener_departamentos()
    
#     if not result["success"]:
#         raise HTTPException(status_code=500, detail=result["error"])
    
#     return result

# @router.get("/estados")
# async def obtener_estados(token: str = Depends(verify_token)):
#     """
#     Obtener lista de estados disponibles
    
#     - Retorna: Lista de todos los estados posibles de contribuyentes
#     """
#     result = ruc_service.obtener_estados()
    
#     if not result["success"]:
#         raise HTTPException(status_code=500, detail=result["error"])
    
#     return result

@router.post("/exportar")
async def exportar_datos(
    request: ExportRequest,
    token: str = Depends(verify_token)
):
    """
    Exportar datos de un contribuyente en diferentes formatos
    
    - **ruc**: RUC de 11 dígitos del contribuyente
    - **format**: Formato de exportación (json, csv, texto)
    - Retorna: Datos del contribuyente en el formato solicitado
    """
    result = ruc_service.buscar_por_ruc(request.ruc, source="api")
    
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["error"])
    
    data = result["data"]
    
    if request.format.lower() == "json":
        return {
            "success": True,
            "format": "json",
            "filename": f"ruc_{request.ruc}.json",
            "data": data,
            "timestamp": result["timestamp"]
        }
    
    elif request.format.lower() == "csv":
        csv_lines = []
        csv_lines.append(",".join(data.keys()))
        csv_lines.append(",".join(str(v) for v in data.values()))
        csv_content = "\n".join(csv_lines)
        
        return {
            "success": True,
            "format": "csv",
            "filename": f"ruc_{request.ruc}.csv",
            "data": csv_content,
            "timestamp": result["timestamp"]
        }
    
    elif request.format.lower() == "texto":
        text_lines = [f"{k}: {v}" for k, v in data.items()]
        text_content = "\n".join(text_lines)
        
        return {
            "success": True,
            "format": "texto",
            "filename": f"ruc_{request.ruc}.txt",
            "data": text_content,
            "timestamp": result["timestamp"]
        }
    
    else:
        raise HTTPException(
            status_code=400, 
            detail=f"Formato '{request.format}' no soportado. Formatos disponibles: json, csv, texto"
        )

# Endpoint para verificar token (opcional)
@router.get("/verify-token")
async def verify_token_endpoint(token: str = Depends(verify_token)):
    """
    Verificar si el token es válido
    
    - Retorna: Estado de validez del token
    """
    return {
        "success": True,
        "message": "Token válido",
        "token_valid": True,
        "api_title": settings.API_TITLE,
        "version": settings.API_VERSION
    }