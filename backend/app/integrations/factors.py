"""
ABM Reporter - Factors.ai Integration
Handles website session analytics and account identification
"""
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import httpx

from ..config import get_settings
from ..models.account import WebsiteMetrics

logger = logging.getLogger(__name__)


class FactorsClient:
    """Factors.ai API client for website analytics"""

    # Note: Factors.ai API endpoint - update based on actual documentation
    BASE_URL = "https://api.factors.ai/v1"

    def __init__(self):
        self.settings = get_settings()
        self._api_key = self.settings.FACTORS_API_KEY
        self._project_id = self.settings.FACTORS_PROJECT_ID

    def _get_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        }

    async def _make_request(
            self,
            method: str,
            endpoint: str,
            params: Optional[Dict] = None,
            json_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to Factors.ai API"""
        url = f"{self.BASE_URL}{endpoint}"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    params=params,
                    json=json_data,
                    timeout=30.0
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Factors.ai API error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Factors.ai request error: {e}")
                raise

    async def get_identified_accounts(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of identified accounts from website traffic
        Factors.ai uses IP-to-company matching to identify B2B visitors
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = f"/projects/{self._project_id}/accounts"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "limit": 1000
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            accounts = result.get('accounts', [])
            logger.info(f"Fetched {len(accounts)} identified accounts from Factors.ai")
            return accounts
        except Exception as e:
            logger.error(f"Error fetching identified accounts: {e}")
            raise

    async def get_account_sessions(
            self,
            account_domain: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> WebsiteMetrics:
        """
        Get website session metrics for a specific account/company
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = f"/projects/{self._project_id}/accounts/{account_domain}/sessions"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d")
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)

            return WebsiteMetrics(
                sessions=result.get('total_sessions', 0),
                page_views=result.get('total_page_views', 0),
                avg_session_duration=result.get('avg_session_duration', 0.0),
                bounce_rate=result.get('bounce_rate', 0.0),
                unique_visitors=result.get('unique_visitors', 0)
            )
        except Exception as e:
            logger.error(f"Error fetching sessions for {account_domain}: {e}")
            raise

    async def get_all_account_sessions(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> Dict[str, WebsiteMetrics]:
        """
        Get session metrics for all identified accounts
        Returns: {domain: WebsiteMetrics}
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = f"/projects/{self._project_id}/accounts/sessions"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "group_by": "domain"
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)

            account_metrics = {}
            for account in result.get('accounts', []):
                domain = account.get('domain', '')
                if domain:
                    account_metrics[domain] = WebsiteMetrics(
                        sessions=account.get('sessions', 0),
                        page_views=account.get('page_views', 0),
                        avg_session_duration=account.get('avg_duration', 0.0),
                        bounce_rate=account.get('bounce_rate', 0.0),
                        unique_visitors=account.get('unique_visitors', 0)
                    )

            logger.info(f"Fetched session metrics for {len(account_metrics)} accounts")
            return account_metrics
        except Exception as e:
            logger.error(f"Error fetching all account sessions: {e}")
            raise

    async def get_page_views_by_account(
            self,
            account_domain: str,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get page view details for a specific account
        Shows which pages they visited, time on page, etc.
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = f"/projects/{self._project_id}/accounts/{account_domain}/pageviews"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d")
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            page_views = result.get('pageviews', [])
            logger.info(f"Fetched {len(page_views)} page views for {account_domain}")
            return page_views
        except Exception as e:
            logger.error(f"Error fetching page views for {account_domain}: {e}")
            raise

    async def get_account_journey(self, account_domain: str) -> List[Dict[str, Any]]:
        """
        Get the full journey/timeline for an account
        Shows all touchpoints and interactions over time
        """
        endpoint = f"/projects/{self._project_id}/accounts/{account_domain}/journey"

        try:
            result = await self._make_request("GET", endpoint)
            journey = result.get('events', [])
            logger.info(f"Fetched {len(journey)} journey events for {account_domain}")
            return journey
        except Exception as e:
            logger.error(f"Error fetching journey for {account_domain}: {e}")
            raise

    async def get_intent_signals(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get intent signals for accounts
        High engagement or specific page visits may indicate intent
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        endpoint = f"/projects/{self._project_id}/intent-signals"
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d")
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            signals = result.get('signals', [])
            logger.info(f"Fetched {len(signals)} intent signals")
            return signals
        except Exception as e:
            logger.error(f"Error fetching intent signals: {e}")
            raise


# Singleton instance
_factors_client: Optional[FactorsClient] = None


def get_factors_client() -> FactorsClient:
    """Get or create Factors.ai client singleton"""
    global _factors_client
    if _factors_client is None:
        _factors_client = FactorsClient()
    return _factors_client
