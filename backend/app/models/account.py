"""
ABM Reporter - Account Data Models
Defines the core data structures for ABM reporting
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class OpportunityStage(str, Enum):
    """Salesforce opportunity stages"""
    PROSPECTING = "Prospecting"
    QUALIFICATION = "Qualification"
    NEEDS_ANALYSIS = "Needs Analysis"
    VALUE_PROPOSITION = "Value Proposition"
    PROPOSAL = "Proposal/Price Quote"
    NEGOTIATION = "Negotiation/Review"
    CLOSED_WON = "Closed Won"
    CLOSED_LOST = "Closed Lost"


class Contact(BaseModel):
    """Contact from CRM"""
    id: str
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    title: Optional[str] = None
    source: str  # "sfdc" or "hubspot"
    account_id: Optional[str] = None
    created_at: Optional[datetime] = None


class Opportunity(BaseModel):
    """Salesforce Opportunity"""
    id: str
    name: str
    amount: Optional[float] = None
    stage: str
    close_date: Optional[datetime] = None
    is_won: bool = False
    is_closed: bool = False
    account_id: str
    created_at: Optional[datetime] = None


class LinkedInMetrics(BaseModel):
    """LinkedIn engagement metrics"""
    organic_impressions: int = 0
    organic_clicks: int = 0
    organic_engagement_rate: float = 0.0
    ad_impressions: int = 0
    ad_clicks: int = 0
    ad_spend: float = 0.0
    ad_ctr: float = 0.0


class WebsiteMetrics(BaseModel):
    """Website analytics from Factors.ai"""
    sessions: int = 0
    page_views: int = 0
    avg_session_duration: float = 0.0
    bounce_rate: float = 0.0
    unique_visitors: int = 0


class FormSubmission(BaseModel):
    """HubSpot form submission"""
    id: str
    form_name: str
    submitted_at: datetime
    contact_email: Optional[str] = None
    page_url: Optional[str] = None


class AccountEngagement(BaseModel):
    """Aggregated account-level engagement data"""
    account_name: str
    domains: List[str] = Field(default_factory=list)

    # Contact metrics
    sfdc_contacts: int = 0
    hubspot_contacts: int = 0
    total_contacts: int = 0

    # LinkedIn metrics
    linkedin_organic_impressions: int = 0
    linkedin_ad_impressions: int = 0
    linkedin_total_impressions: int = 0
    linkedin_engagement_rate: float = 0.0

    # Website metrics
    website_sessions: int = 0
    website_page_views: int = 0

    # Form submissions
    form_submissions: int = 0

    # Pipeline metrics
    current_opportunities: int = 0
    closed_won: int = 0
    closed_lost: int = 0
    open_opportunities: int = 0
    pipeline_value: float = 0.0

    # Enrichment data
    industry: Optional[str] = None
    employee_count: Optional[int] = None
    annual_revenue: Optional[float] = None

    # Intent signals (from 6sense/Bombora if integrated)
    intent_score: Optional[int] = None
    intent_topics: List[str] = Field(default_factory=list)

    # Timestamps
    last_updated: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "account_name": "Myntra",
                "domains": ["myntra.com", "myntra.in"],
                "sfdc_contacts": 20,
                "hubspot_contacts": 45,
                "total_contacts": 65,
                "linkedin_organic_impressions": 55,
                "linkedin_ad_impressions": 100,
                "linkedin_total_impressions": 155,
                "website_sessions": 85,
                "form_submissions": 4,
                "current_opportunities": 2,
                "closed_lost": 1,
                "open_opportunities": 1,
                "pipeline_value": 145000.0
            }
        }


class AccountList(BaseModel):
    """List of accounts for API response"""
    accounts: List[AccountEngagement]
    total_count: int
    last_synced: Optional[datetime] = None


class AccountFilter(BaseModel):
    """Filters for querying accounts"""
    min_pipeline: Optional[float] = None
    max_pipeline: Optional[float] = None
    min_contacts: Optional[int] = None
    has_open_opportunities: Optional[bool] = None
    industries: Optional[List[str]] = None
    min_intent_score: Optional[int] = None
    search_query: Optional[str] = None
    sort_by: str = "pipeline_value"
    sort_order: str = "desc"
    page: int = 1
    page_size: int = 50
