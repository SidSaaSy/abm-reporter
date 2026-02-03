"""
ABM Reporter - LinkedIn Integration
Handles LinkedIn Ads API and Organic Page analytics
"""
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import httpx

from ..config import get_settings
from ..models.account import LinkedInMetrics

logger = logging.getLogger(__name__)


class LinkedInClient:
    """LinkedIn Marketing API client for ABM data"""

    BASE_URL = "https://api.linkedin.com/v2"
    ADS_BASE_URL = "https://api.linkedin.com/rest"

    def __init__(self):
        self.settings = get_settings()
        self._access_token = self.settings.LINKEDIN_ACCESS_TOKEN
        self._organization_id = self.settings.LINKEDIN_ORGANIZATION_ID
        self._ad_account_id = self.settings.LINKEDIN_AD_ACCOUNT_ID

    def _get_headers(self, use_rest_api: bool = False) -> Dict[str, str]:
        """Get authentication headers"""
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0"
        }

        if use_rest_api:
            headers["LinkedIn-Version"] = "202401"

        return headers

    async def _make_request(
            self,
            method: str,
            endpoint: str,
            params: Optional[Dict] = None,
            json_data: Optional[Dict] = None,
            use_rest_api: bool = False
    ) -> Dict[str, Any]:
        """Make authenticated request to LinkedIn API"""
        base = self.ADS_BASE_URL if use_rest_api else self.BASE_URL
        url = f"{base}{endpoint}"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(use_rest_api),
                    params=params,
                    json=json_data,
                    timeout=30.0
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"LinkedIn API error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"LinkedIn request error: {e}")
                raise

    async def get_organization_page_statistics(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get organic page statistics for the organization
        Includes impressions, clicks, engagement
        """
        if not self._organization_id:
            raise ValueError("LinkedIn Organization ID not configured")

        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        # Format timestamps for LinkedIn API (milliseconds)
        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)

        endpoint = "/organizationalEntityShareStatistics"
        params = {
            "q": "organizationalEntity",
            "organizationalEntity": f"urn:li:organization:{self._organization_id}",
            "timeIntervals.timeGranularityType": "DAY",
            "timeIntervals.timeRange.start": start_ts,
            "timeIntervals.timeRange.end": end_ts
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            logger.info("Fetched LinkedIn organic page statistics")
            return result
        except Exception as e:
            logger.error(f"Error fetching page statistics: {e}")
            raise

    async def get_page_follower_statistics(self) -> Dict[str, Any]:
        """Get follower statistics by various dimensions"""
        if not self._organization_id:
            raise ValueError("LinkedIn Organization ID not configured")

        endpoint = "/organizationalEntityFollowerStatistics"
        params = {
            "q": "organizationalEntity",
            "organizationalEntity": f"urn:li:organization:{self._organization_id}"
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            return result
        except Exception as e:
            logger.error(f"Error fetching follower statistics: {e}")
            raise

    async def get_ad_campaigns(self) -> List[Dict[str, Any]]:
        """Get all ad campaigns for the ad account"""
        if not self._ad_account_id:
            raise ValueError("LinkedIn Ad Account ID not configured")

        endpoint = "/adCampaigns"
        params = {
            "q": "search",
            "search": f"(account:(values:List(urn:li:sponsoredAccount:{self._ad_account_id})))"
        }

        try:
            result = await self._make_request("GET", endpoint, params=params, use_rest_api=True)
            campaigns = result.get('elements', [])
            logger.info(f"Fetched {len(campaigns)} LinkedIn ad campaigns")
            return campaigns
        except Exception as e:
            logger.error(f"Error fetching ad campaigns: {e}")
            raise

    async def get_ad_analytics(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            campaign_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get ad analytics (impressions, clicks, spend, etc.)
        """
        if not self._ad_account_id:
            raise ValueError("LinkedIn Ad Account ID not configured")

        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = "/adAnalytics"
        params = {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "dateRange.start.day": start_date.day,
            "dateRange.start.month": start_date.month,
            "dateRange.start.year": start_date.year,
            "dateRange.end.day": end_date.day,
            "dateRange.end.month": end_date.month,
            "dateRange.end.year": end_date.year,
            "timeGranularity": "ALL",
            "accounts": f"urn:li:sponsoredAccount:{self._ad_account_id}",
            "fields": "impressions,clicks,costInLocalCurrency,dateRange"
        }

        if campaign_ids:
            params["campaigns"] = ",".join([f"urn:li:sponsoredCampaign:{cid}" for cid in campaign_ids])

        try:
            result = await self._make_request("GET", endpoint, params=params, use_rest_api=True)
            analytics = result.get('elements', [])
            logger.info(f"Fetched analytics for {len(analytics)} campaigns")
            return analytics
        except Exception as e:
            logger.error(f"Error fetching ad analytics: {e}")
            raise

    async def get_company_engagement(
            self,
            company_name: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> LinkedInMetrics:
        """
        Get aggregated LinkedIn metrics for a specific company
        Note: This requires LinkedIn Page Insights access for member company data
        """
        # Get organic stats
        try:
            organic_stats = await self.get_organization_page_statistics(start_date, end_date)

            total_impressions = 0
            total_clicks = 0
            total_engagement = 0

            for element in organic_stats.get('elements', []):
                stats = element.get('totalShareStatistics', {})
                total_impressions += stats.get('impressionCount', 0)
                total_clicks += stats.get('clickCount', 0)
                total_engagement += stats.get('engagement', 0)

            engagement_rate = (total_engagement / total_impressions * 100) if total_impressions > 0 else 0

        except Exception as e:
            logger.warning(f"Could not fetch organic stats: {e}")
            total_impressions = 0
            total_clicks = 0
            engagement_rate = 0

        # Get ad stats
        try:
            ad_stats = await self.get_ad_analytics(start_date, end_date)

            ad_impressions = sum(el.get('impressions', 0) for el in ad_stats)
            ad_clicks = sum(el.get('clicks', 0) for el in ad_stats)
            ad_spend = sum(el.get('costInLocalCurrency', 0) for el in ad_stats)
            ad_ctr = (ad_clicks / ad_impressions * 100) if ad_impressions > 0 else 0

        except Exception as e:
            logger.warning(f"Could not fetch ad stats: {e}")
            ad_impressions = 0
            ad_clicks = 0
            ad_spend = 0
            ad_ctr = 0

        return LinkedInMetrics(
            organic_impressions=total_impressions,
            organic_clicks=total_clicks,
            organic_engagement_rate=engagement_rate,
            ad_impressions=ad_impressions,
            ad_clicks=ad_clicks,
            ad_spend=ad_spend,
            ad_ctr=ad_ctr
        )

    async def get_company_page_followers_by_company(self) -> Dict[str, int]:
        """
        Get follower breakdown by company
        This shows which companies your page followers work at
        """
        if not self._organization_id:
            raise ValueError("LinkedIn Organization ID not configured")

        endpoint = "/organizationalEntityFollowerStatistics"
        params = {
            "q": "organizationalEntity",
            "organizationalEntity": f"urn:li:organization:{self._organization_id}"
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)

            # Parse follower counts by company
            company_followers = {}
            for element in result.get('elements', []):
                follower_counts = element.get('followerCountsByAssociationType', [])
                for fc in follower_counts:
                    if fc.get('associationType') == 'COMPANY':
                        company_urn = fc.get('organizationalEntity', '')
                        count = fc.get('followerCounts', {}).get('organicFollowerCount', 0)
                        if company_urn:
                            company_followers[company_urn] = count

            return company_followers
        except Exception as e:
            logger.error(f"Error fetching follower breakdown: {e}")
            raise


# Singleton instance
_linkedin_client: Optional[LinkedInClient] = None


def get_linkedin_client() -> LinkedInClient:
    """Get or create LinkedIn client singleton"""
    global _linkedin_client
    if _linkedin_client is None:
        _linkedin_client = LinkedInClient()
    return _linkedin_client
