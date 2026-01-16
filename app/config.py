import os
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Configuración de la aplicación"""
    
    # Database
    DATABASE_URL: str = "contribuyentes.db"
    
    # API
    API_TITLE: str = "Sistema de Búsqueda de RUC"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API para consulta del padrón SUNAT"
    
    # Cache
    CACHE_SIZE: int = 10000
    CACHE_TTL: int = 3600  # 1 hora en segundos
    
    # Web
    WEB_TITLE: str = "Busqueda RUC SUNAT"
    WEB_DESCRIPTION: str = "Sistema de consulta del padrón de contribuyentes"
    
    # Security
    RATE_LIMIT_PER_MINUTE: int = 100

    # API Token
    API_TOKEN: str 
    
    class Config:
        env_file = ".env"

settings = Settings()