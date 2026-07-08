from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    DATABASE_URL: str = "data/contribuyentes.db"
    CACHE_SIZE: int = 10000
    CACHE_TTL: int = 3600
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "logs"


settings = Settings()
