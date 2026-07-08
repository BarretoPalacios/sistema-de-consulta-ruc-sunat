from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    API_TITLE: str = "Sistema de Búsqueda de RUC"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API para consulta del padrón SUNAT"
    API_TOKEN: str = "your-secret-key"
    CORS_ORIGINS: str = "*"
    RATE_LIMIT_PER_MINUTE: int = 100
    WEB_TITLE: str = "Busqueda RUC SUNAT"
    WEB_DESCRIPTION: str = "Sistema de consulta del padrón de contribuyentes"


api_settings = APISettings()
