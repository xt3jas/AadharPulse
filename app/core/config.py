from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    
    
    APP_NAME: str = "AadharPulse"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_PREFIX: str = "/api/v1"
    
    DATA_DIR: Path = Path(__file__).parent.parent.parent / "data"
    
    @property
    def BRONZE_PATH(self) -> Path:
        
        return self.DATA_DIR / "bronze"
    
    @property
    def SILVER_PATH(self) -> Path:
        
        return self.DATA_DIR / "silver"
    
    @property
    def GOLD_PATH(self) -> Path:
        
        return self.DATA_DIR / "gold"
    
    OVS_ROLLING_WINDOW: int = 30
    
    OVS_CAMP_THRESHOLD: float = 4.0
    MII_HOTSPOT_THRESHOLD: float = 0.40
    DHR_FRAUD_THRESHOLD: float = 1.5
    WEEKEND_OPTIMIZATION_THRESHOLD: float = 0.60
    
    MIN_VOLUME_FOR_CAMP_FLAG: int = 500
    MIN_ENROLMENT_FOR_MIGRATION_FLAG: int = 100
    MIN_TRANSACTIONS_FOR_FRAUD_FLAG: int = 1000
    
    KMEANS_CLUSTERS: int = 3
    RANDOM_STATE: int = 42
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    
    return Settings()

def ensure_data_directories():
    
    settings = get_settings()
    settings.BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    settings.SILVER_PATH.mkdir(parents=True, exist_ok=True)
    settings.GOLD_PATH.mkdir(parents=True, exist_ok=True)
