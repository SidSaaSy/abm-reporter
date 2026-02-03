"""
ABM Reporter - Configuration Settings
Loads environment variables for all API integrations
"""
from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "ABM Reporter"
    DEBUG: bool = False
    API_V1_PREFIX: str = "/api/v1"

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://localhost/abm_reporter"
    REDIS_URL: str = "redis://localhost:6379"

    # Salesforce Integration
    SFDC_CLIENT_ID: Optional[str] = None
    SFDC_CLIENT_SECRET: Optional[str] = None
    SFDC_USERNAME: Optional[str] = None
    SFDC_PASSWORD: Optional[str] = None
    SFDC_SECURITY_TOKEN: Optional[str] = None
    SFDC_DOMAIN: str = "login"  # or "test" for sandbox

    # HubSpot Integration
    HUBSPOT_ACCESS_TOKEN: Optional[str] = None
    HUBSPOT_CLIENT_ID: Optional[str] = None
    HUBSPOT_CLIENT_SECRET: Optional[str] = None
    HUBSPOT_REFRESH_TOKEN: Optional[str] = None

    # LinkedIn Integration
    LINKEDIN_CLIENT_ID: Optional[str] = None
    LINKEDIN_CLIENT_SECRET: Optional[str] = None
    LINKEDIN_ACCESS_TOKEN: Optional[str] = None
    LINKEDIN_ORGANIZATION_ID: Optional[str] = None

    # LinkedIn Ads
    LINKEDIN_AD_ACCOUNT_ID: Optional[str] = None

    # Factors.ai Integration
    FACTORS_API_KEY: Optional[str] = None
    FACTORS_PROJECT_ID: Optional[str] = None

    # 6sense Integration (optional)
    SIXSENSE_API_KEY: Optional[str] = None

    # Clearbit Integration (optional)
    CLEARBIT_API_KEY: Optional[str] = None

    # Cache settings
    CACHE_TTL_SECONDS: int = 300  # 5 minutes default

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance"""
    return Settings()
