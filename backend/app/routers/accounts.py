"""
ABM Reporter - Account API Routes
"""
from fastapi import APIRouter, Depends, Query, HTTPException, UploadFile, File
from typing import Optional, List, Any
from datetime import datetime, timedelta
from pydantic import BaseModel
from ..models.account import AccountEngagement, AccountList, AccountFilter
from ..services.aggregator import get_aggregator, ABMDataAggregator
from ..integrations.csv_handler import get_csv_handler

router = APIRouter(prefix="/accounts", tags=["accounts"])


class AccountListWithSummary(BaseModel):
    """Account list with summary statistics"""
    accounts: List[AccountEngagement]
    total_count: int
    last_synced: datetime
    # Summary stats from ALL accounts (not just paginated)
    summary: dict


@router.get("/", response_model=AccountListWithSummary)
async def get_accounts(
        search: Optional[str] = Query(None, description="Search by account name or domain"),
        min_pipeline: Optional[float] = Query(None, description="Minimum pipeline value"),
        max_pipeline: Optional[float] = Query(None, description="Maximum pipeline value"),
        min_contacts: Optional[int] = Query(None, description="Minimum total contacts"),
        has_open_opportunities: Optional[bool] = Query(None, description="Filter by open opportunities"),
        industries: Optional[str] = Query(None, description="Comma-separated list of industries"),
        sort_by: str = Query("pipeline_value", description="Sort field"),
        sort_order: str = Query("desc", description="Sort order (asc/desc)"),
        page: int = Query(1, ge=1, description="Page number"),
        page_size: int = Query(50, ge=1, le=100, description="Page size"),
        refresh: bool = Query(False, description="Force refresh data"),
        aggregator: ABMDataAggregator = Depends(get_aggregator)
):
    """
    Get aggregated account data from all integrated sources
    """
    # Parse industries
    industry_list = industries.split(',') if industries else None
    
    # Build filters
    filters = AccountFilter(
        search_query=search,
        min_pipeline=min_pipeline,
        max_pipeline=max_pipeline,
        min_contacts=min_contacts,
        has_open_opportunities=has_open_opportunities,
        industries=industry_list,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size
    )
    
    # Get aggregated data
    all_data = await aggregator.aggregate_account_data(force_refresh=refresh)
    
    # Calculate summary stats from ALL accounts BEFORE pagination
    all_accounts = all_data.accounts
    total_pipeline = sum(a.pipeline_value for a in all_accounts)
    total_contacts = sum(a.total_contacts for a in all_accounts)
    total_sfdc_contacts = sum(a.sfdc_contacts for a in all_accounts)
    total_hubspot_contacts = sum(a.hubspot_contacts for a in all_accounts)
    total_sessions = sum(a.website_sessions for a in all_accounts)
    total_submissions = sum(a.form_submissions for a in all_accounts)
    accounts_with_opps = len([a for a in all_accounts if a.open_opportunities > 0])
    
    # Apply filters (includes pagination)
    filtered_accounts = aggregator.filter_accounts(all_accounts, filters)
    
    return AccountListWithSummary(
        accounts=filtered_accounts,
        total_count=len(all_accounts),
        last_synced=all_data.last_synced,
        summary={
            "total_accounts": len(all_accounts),
            "total_pipeline": total_pipeline,
            "total_contacts": total_contacts,
            "total_sfdc_contacts": total_sfdc_contacts,
            "total_hubspot_contacts": total_hubspot_contacts,
            "total_website_sessions": total_sessions,
            "total_form_submissions": total_submissions,
            "accounts_with_open_opportunities": accounts_with_opps
        }
    )


@router.get("/{account_name}", response_model=AccountEngagement)
async def get_account_detail(
        account_name: str,
        aggregator: ABMDataAggregator = Depends(get_aggregator)
):
    """
    Get detailed data for a specific account
    """
    all_data = await aggregator.aggregate_account_data()
    
    for account in all_data.accounts:
        if account.account_name.lower() == account_name.lower():
            return account
    
    raise HTTPException(status_code=404, detail=f"Account '{account_name}' not found")


@router.get("/{account_name}/contacts")
async def get_account_contacts(
        account_name: str,
        source: Optional[str] = Query(None, description="Filter by source (sfdc/hubspot)")
):
    """
    Get contacts for a specific account
    """
    return {"message": "Contact details endpoint - requires account ID mapping"}


@router.get("/{account_name}/opportunities")
async def get_account_opportunities(account_name: str):
    """
    Get opportunities for a specific account
    """
    return {"message": "Opportunities endpoint - requires SFDC account ID"}


@router.get("/{account_name}/engagement")
async def get_account_engagement_timeline(
        account_name: str,
        days: int = Query(30, description="Number of days of history")
):
    """
    Get engagement timeline for a specific account
    """
    return {"message": "Engagement timeline endpoint"}


@router.post("/upload/fibbler")
async def upload_fibbler_data(
        file: UploadFile = File(..., description="Fibbler CSV export")
):
    """
    Upload Fibbler CSV export to supplement LinkedIn engagement data
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    csv_handler = get_csv_handler()
    
    try:
        content = await file.read()
        data = csv_handler.parse_fibbler_csv(content)
        return {
            "message": f"Successfully parsed {len(data)} accounts from Fibbler export",
            "accounts": data[:10]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/upload/linkedin-ads")
async def upload_linkedin_ads_data(
        file: UploadFile = File(..., description="LinkedIn Ads CSV export")
):
    """
    Upload LinkedIn Ads CSV export
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    csv_handler = get_csv_handler()
    
    try:
        content = await file.read()
        data = csv_handler.parse_linkedin_ads_csv(content)
        return {
            "message": f"Successfully parsed {len(data)} records from LinkedIn Ads export",
            "records": data[:10]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/refresh")
async def refresh_data(
        aggregator: ABMDataAggregator = Depends(get_aggregator)
):
    """
    Force refresh all data from integrated sources
    """
    aggregator.invalidate_cache()
    data = await aggregator.aggregate_account_data(force_refresh=True)
    
    return {
        "message": "Data refreshed successfully",
        "total_accounts": data.total_count,
        "last_synced": data.last_synced
    }


@router.get("/summary/stats")
async def get_summary_stats(
        aggregator: ABMDataAggregator = Depends(get_aggregator)
):
    """
    Get summary statistics across all accounts
    """
    data = await aggregator.aggregate_account_data()

    total_pipeline = sum(a.pipeline_value for a in data.accounts)
    total_contacts = sum(a.total_contacts for a in data.accounts)
    total_sessions = sum(a.website_sessions for a in data.accounts)
    total_submissions = sum(a.form_submissions for a in data.accounts)
    accounts_with_opps = len([a for a in data.accounts if a.open_opportunities > 0])
    total_open_opps = sum(a.open_opportunities for a in data.accounts)
    total_closed_won = sum(a.closed_won for a in data.accounts)
    total_closed_lost = sum(a.closed_lost for a in data.accounts)

    return {
        "total_accounts": data.total_count,
        "total_pipeline": total_pipeline,
        "total_contacts": total_contacts,
        "total_website_sessions": total_sessions,
        "total_form_submissions": total_submissions,
        "accounts_with_open_opportunities": accounts_with_opps,
        "total_open_opportunities": total_open_opps,
        "total_closed_won": total_closed_won,
        "total_closed_lost": total_closed_lost,
        "avg_contacts_per_account": total_contacts / data.total_count if data.total_count > 0 else 0,
        "last_synced": data.last_synced
    }
