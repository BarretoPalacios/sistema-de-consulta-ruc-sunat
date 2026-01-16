from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import os

# Crear directorios necesarios
os.makedirs("static/css", exist_ok=True)
os.makedirs("static/js", exist_ok=True)
os.makedirs("app/web/templates", exist_ok=True)

app = FastAPI(
    title="Sistema de Búsqueda de RUC",
    description="API y Web Interface para consulta del padrón SUNAT",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Montar archivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configurar templates
templates = Jinja2Templates(directory="app/web/templates")

# Importar routers después de crear la app
from app.api.endpoints import router as api_router
from app.web.routes import router as web_router

# Incluir routers
app.include_router(api_router, prefix="/api/v1", tags=["API"])
# app.include_router(web_router, tags=["Web"])

# @app.get("/", response_class=HTMLResponse)
# async def root(request: Request):
#     """Página principal"""
#     return templates.TemplateResponse("index.html", {
#         "request": request,
#         "title": "Sistema de Búsqueda de RUC",
#         "page": "home"
#     })

# @app.get("/health")
# async def health_check():
#     """Endpoint de salud"""
#     return {"status": "healthy", "service": "ruc-search"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1
    )