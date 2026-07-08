from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from apps.api.app.core.config import api_settings

security = HTTPBearer()


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != api_settings.API_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    return credentials.credentials
