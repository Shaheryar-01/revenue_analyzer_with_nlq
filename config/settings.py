# app/config/settings.py
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import List

class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str
    
    # Supabase
    supabase_url: str
    supabase_key: str
    
    # App settings
    debug: bool = False
    cors_origins: str = "http://localhost:3000"
    
    # File upload
    max_file_size_mb: int = 50
    allowed_extensions: List[str] = ['.xlsx', '.xls']
    
    @property
    def cors_origins_list(self) -> List[str]:
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()