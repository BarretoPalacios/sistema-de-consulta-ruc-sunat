import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from apps.api.app.api.ruc.router import router as api_router
from apps.api.app.core.config import api_settings
from apps.api.app.core.limiter import limiter
from apps.api.app.web.routes import router as web_router

app = FastAPI(
    title=api_settings.API_TITLE,
    description=api_settings.API_DESCRIPTION,
    version=api_settings.API_VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=api_settings.CORS_ORIGINS.split(",") if api_settings.CORS_ORIGINS != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(api_router, prefix="/api", tags=["API"])
app.include_router(web_router, tags=["Web"])


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "ruc-api"}


if __name__ == "__main__":
    uvicorn.run("apps.api.main:app", host="0.0.0.0", port=8000, reload=True, workers=1)
