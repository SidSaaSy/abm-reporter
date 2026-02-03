"""
ABM Reporter - Main FastAPI Application
Account-Based Marketing Dashboard API
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from .config import get_settings
from .routers import accounts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler"""
    # Startup
    logger.info("Starting ABM Reporter API...")
    settings = get_settings()
    logger.info(f"Running in {'debug' if settings.DEBUG else 'production'} mode")
    yield
    # Shutdown
    logger.info("Shutting down ABM Reporter API...")


# Create FastAPI application
app = FastAPI(
    title="ABM Reporter",
    description="""
    Account-Based Marketing Dashboard API

    Aggregates data from multiple sources to provide unified account-level insights:
    - **Salesforce**: Accounts, Contacts, Opportunities
    - **HubSpot**: Companies, Contacts, Form Submissions
    - **LinkedIn**: Organic Page Stats, Ad Analytics
    - **Factors.ai**: Website Sessions, Account Identification

    ## Features
    - Real-time data aggregation
    - Account-level engagement metrics
    - Pipeline tracking
    - Multi-channel touchpoint visibility
    """,
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(accounts.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "ABM Reporter API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    settings = get_settings()

    # Check which integrations are configured
    integrations = {
        "salesforce": bool(settings.SFDC_USERNAME and settings.SFDC_PASSWORD),
        "hubspot": bool(settings.HUBSPOT_ACCESS_TOKEN),
        "linkedin": bool(settings.LINKEDIN_ACCESS_TOKEN),
        "factors": bool(settings.FACTORS_API_KEY),
    }

    return {
        "status": "healthy",
        "integrations": integrations
    }


@app.get("/api/v1/integrations/status")
async def get_integration_status():
    """Get status of all integrations"""
    settings = get_settings()

    return {
        "salesforce": {
            "configured": bool(settings.SFDC_USERNAME and settings.SFDC_PASSWORD),
            "domain": settings.SFDC_DOMAIN
        },
        "hubspot": {
            "configured": bool(settings.HUBSPOT_ACCESS_TOKEN),
        },
        "linkedin": {
            "configured": bool(settings.LINKEDIN_ACCESS_TOKEN),
            "organization_id": settings.LINKEDIN_ORGANIZATION_ID,
            "ad_account_id": settings.LINKEDIN_AD_ACCOUNT_ID
        },
        "factors": {
            "configured": bool(settings.FACTORS_API_KEY),
            "project_id": settings.FACTORS_PROJECT_ID
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
