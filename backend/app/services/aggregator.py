"""
ABM Reporter - Data Aggregation Service
Combines data from all integrations into unified account-level view
"""
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import asyncio

from ..models.account import AccountEngagement, AccountList, AccountFilter
from ..integrations.salesforce import get_salesforce_client
from ..integrations.hubspot import get_hubspot_client
from ..integrations.linkedin import get_linkedin_client
from ..integrations.factors import get_factors_client

logger = logging.getLogger(__name__)


class ABMDataAggregator:
    """
    Aggregates ABM data from multiple sources into unified account view
    """

    def __init__(self):
        self.sfdc = get_salesforce_client()
        self.hubspot = get_hubspot_client()
        self.linkedin = get_linkedin_client()
        self.factors = get_factors_client()

        # Cache for aggregated data
        self._cache: Dict[str, Any] = {}
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=5)

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid"""
        if not self._cache_timestamp:
            return False
        return datetime.utcnow() - self._cache_timestamp < self._cache_ttl

    async def _fetch_salesforce_data(self) -> Dict[str, Any]:
        """Fetch all Salesforce data"""
        try:
            accounts = await self.sfdc.get_accounts()
            contact_counts = await self.sfdc.get_contacts_count_by_account()
            opp_summary = await self.sfdc.get_opportunity_summary()

            return {
                'accounts': accounts,
                'contact_counts': contact_counts,
                'opportunity_summary': opp_summary
            }
        except Exception as e:
            logger.error(f"Error fetching Salesforce data: {e}")
            return {'accounts': [], 'contact_counts': {}, 'opportunity_summary': {}}

    async def _fetch_hubspot_data(self) -> Dict[str, Any]:
        """Fetch all HubSpot data"""
        try:
            companies = await self.hubspot.get_companies()
            contact_counts = await self.hubspot.get_contacts_count_by_company_domain()
            forms = await self.hubspot.get_forms()
            submissions = await self.hubspot.get_form_submissions()

            return {
                'companies': companies,
                'contact_counts': contact_counts,
                'forms': forms,
                'form_submissions': submissions
            }
        except Exception as e:
            logger.error(f"Error fetching HubSpot data: {e}")
            return {'companies': [], 'contact_counts': {}, 'forms': [], 'form_submissions': []}

    async def _fetch_linkedin_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch LinkedIn data"""
        try:
            organic_stats = await self.linkedin.get_organization_page_statistics(start_date, end_date)
            ad_analytics = await self.linkedin.get_ad_analytics(start_date, end_date)

            return {
                'organic_stats': organic_stats,
                'ad_analytics': ad_analytics
            }
        except Exception as e:
            logger.error(f"Error fetching LinkedIn data: {e}")
            return {'organic_stats': {}, 'ad_analytics': []}

    async def _fetch_factors_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch Factors.ai data"""
        try:
            accounts = await self.factors.get_identified_accounts(start_date, end_date)
            sessions = await self.factors.get_all_account_sessions(start_date, end_date)

            return {
                'identified_accounts': accounts,
                'sessions': sessions
            }
        except Exception as e:
            logger.error(f"Error fetching Factors.ai data: {e}")
            return {'identified_accounts': [], 'sessions': {}}

    async def aggregate_account_data(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            force_refresh: bool = False
    ) -> AccountList:
        """
        Aggregate data from all sources into unified account view
        """
        # Check cache
        if not force_refresh and self._is_cache_valid() and 'aggregated' in self._cache:
            logger.info("Returning cached aggregated data")
            return self._cache['aggregated']

        # Set date range
        if not end_date:
            end_date = datetime.utcnow()
        if not start_date:
            start_date = end_date - timedelta(days=30)

        # Fetch data from all sources in parallel
        sfdc_task = self._fetch_salesforce_data()
        hubspot_task = self._fetch_hubspot_data()
        linkedin_task = self._fetch_linkedin_data(start_date, end_date)
        factors_task = self._fetch_factors_data(start_date, end_date)

        sfdc_data, hubspot_data, linkedin_data, factors_data = await asyncio.gather(
            sfdc_task, hubspot_task, linkedin_task, factors_task,
            return_exceptions=True
        )

        # Handle any exceptions
        if isinstance(sfdc_data, Exception):
            logger.error(f"Salesforce fetch failed: {sfdc_data}")
            sfdc_data = {'accounts': [], 'contact_counts': {}, 'opportunity_summary': {}}
        if isinstance(hubspot_data, Exception):
            logger.error(f"HubSpot fetch failed: {hubspot_data}")
            hubspot_data = {'companies': [], 'contact_counts': {}, 'forms': [], 'form_submissions': []}
        if isinstance(linkedin_data, Exception):
            logger.error(f"LinkedIn fetch failed: {linkedin_data}")
            linkedin_data = {'organic_stats': {}, 'ad_analytics': []}
        if isinstance(factors_data, Exception):
            logger.error(f"Factors.ai fetch failed: {factors_data}")
            factors_data = {'identified_accounts': [], 'sessions': {}}

        # Build unified account list
        accounts = self._merge_account_data(sfdc_data, hubspot_data, linkedin_data, factors_data)

        result = AccountList(
            accounts=accounts,
            total_count=len(accounts),
            last_synced=datetime.utcnow()
        )

        # Update cache
        self._cache['aggregated'] = result
        self._cache_timestamp = datetime.utcnow()

        logger.info(f"Aggregated data for {len(accounts)} accounts")
        return result

    def _merge_account_data(
            self,
            sfdc_data: Dict[str, Any],
            hubspot_data: Dict[str, Any],
            linkedin_data: Dict[str, Any],
            factors_data: Dict[str, Any]
    ) -> List[AccountEngagement]:
        """
        Merge data from all sources into unified account records
        """
        accounts_map: Dict[str, AccountEngagement] = {}

        # Process Salesforce accounts
        for account in sfdc_data.get('accounts', []):
            account_id = account.get('Id')
            name = account.get('Name', 'Unknown')
            website = account.get('Website', '')

            # Extract domain from website
            domains = []
            if website:
                domain = website.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
                domains.append(domain)

            # Get contact count
            sfdc_contacts = sfdc_data.get('contact_counts', {}).get(account_id, 0)

            # Get opportunity data
            opp_data = sfdc_data.get('opportunity_summary', {}).get(account_id, {})

            key = name.lower()
            accounts_map[key] = AccountEngagement(
                account_name=name,
                domains=domains,
                sfdc_contacts=sfdc_contacts,
                hubspot_contacts=0,
                total_contacts=sfdc_contacts,
                linkedin_organic_impressions=0,
                linkedin_ad_impressions=0,
                linkedin_total_impressions=0,
                website_sessions=0,
                form_submissions=0,
                current_opportunities=opp_data.get('open_opps', 0) + opp_data.get('closed_won', 0) + opp_data.get('closed_lost', 0),
                closed_won=opp_data.get('closed_won', 0),
                closed_lost=opp_data.get('closed_lost', 0),
                open_opportunities=opp_data.get('open_opps', 0),
                pipeline_value=opp_data.get('pipeline_value', 0),
                industry=account.get('Industry'),
                employee_count=account.get('NumberOfEmployees'),
                annual_revenue=account.get('AnnualRevenue')
            )

        # Process HubSpot companies
        for company in hubspot_data.get('companies', []):
            props = company.get('properties', {})
            name = props.get('name', 'Unknown')
            domain = props.get('domain', '')

            key = name.lower()

            if key in accounts_map:
                # Update existing account
                if domain and domain not in accounts_map[key].domains:
                    accounts_map[key].domains.append(domain)
            else:
                # Create new account
                accounts_map[key] = AccountEngagement(
                    account_name=name,
                    domains=[domain] if domain else [],
                    sfdc_contacts=0,
                    hubspot_contacts=0,
                    total_contacts=0,
                    linkedin_organic_impressions=0,
                    linkedin_ad_impressions=0,
                    linkedin_total_impressions=0,
                    website_sessions=0,
                    form_submissions=0,
                    current_opportunities=0,
                    closed_won=0,
                    closed_lost=0,
                    open_opportunities=0,
                    pipeline_value=0,
                    industry=props.get('industry'),
                    employee_count=int(props.get('numberofemployees', 0) or 0),
                    annual_revenue=float(props.get('annualrevenue', 0) or 0)
                )

            # Update HubSpot contact count
            company_id = company.get('id')
            hs_contacts = hubspot_data.get('contact_counts', {}).get(company_id, 0)
            accounts_map[key].hubspot_contacts = hs_contacts
            accounts_map[key].total_contacts = accounts_map[key].sfdc_contacts + hs_contacts

        # Process form submissions
        submissions_by_domain: Dict[str, int] = {}
        for submission in hubspot_data.get('form_submissions', []):
            email = submission.contact_email
            if email:
                domain = email.split('@')[-1]
                submissions_by_domain[domain] = submissions_by_domain.get(domain, 0) + 1

        # Match submissions to accounts
        for key, account in accounts_map.items():
            for domain in account.domains:
                if domain in submissions_by_domain:
                    account.form_submissions += submissions_by_domain[domain]

        # Process Factors.ai sessions
        sessions_data = factors_data.get('sessions', {})
        for key, account in accounts_map.items():
            for domain in account.domains:
                if domain in sessions_data:
                    metrics = sessions_data[domain]
                    account.website_sessions = metrics.sessions
                    account.website_page_views = metrics.page_views
                    break

        # Process LinkedIn data (simplified - in reality would need company matching)
        # LinkedIn data is typically org-level, so we'd need additional mapping
        organic_stats = linkedin_data.get('organic_stats', {})
        ad_analytics = linkedin_data.get('ad_analytics', [])

        # For now, aggregate at org level
        total_organic_impressions = 0
        total_ad_impressions = 0

        for element in organic_stats.get('elements', []):
            stats = element.get('totalShareStatistics', {})
            total_organic_impressions += stats.get('impressionCount', 0)

        for element in ad_analytics:
            total_ad_impressions += element.get('impressions', 0)

        # Calculate totals
        for account in accounts_map.values():
            account.linkedin_total_impressions = (
                    account.linkedin_organic_impressions + account.linkedin_ad_impressions
            )

        return list(accounts_map.values())

    def filter_accounts(
            self,
            accounts: List[AccountEngagement],
            filters: AccountFilter
    ) -> List[AccountEngagement]:
        """Apply filters to account list"""
        filtered = accounts

        if filters.min_pipeline is not None:
            filtered = [a for a in filtered if a.pipeline_value >= filters.min_pipeline]

        if filters.max_pipeline is not None:
            filtered = [a for a in filtered if a.pipeline_value <= filters.max_pipeline]

        if filters.min_contacts is not None:
            filtered = [a for a in filtered if a.total_contacts >= filters.min_contacts]

        if filters.has_open_opportunities is not None:
            if filters.has_open_opportunities:
                filtered = [a for a in filtered if a.open_opportunities > 0]
            else:
                filtered = [a for a in filtered if a.open_opportunities == 0]

        if filters.industries:
            filtered = [a for a in filtered if a.industry in filters.industries]

        if filters.min_intent_score is not None:
            filtered = [a for a in filtered if (a.intent_score or 0) >= filters.min_intent_score]

        if filters.search_query:
            query = filters.search_query.lower()
            filtered = [a for a in filtered if query in a.account_name.lower() or
                        any(query in d.lower() for d in a.domains)]

        # Sort
        reverse = filters.sort_order == 'desc'
        sort_key = {
            'pipeline_value': lambda a: a.pipeline_value,
            'total_contacts': lambda a: a.total_contacts,
            'website_sessions': lambda a: a.website_sessions,
            'form_submissions': lambda a: a.form_submissions,
            'account_name': lambda a: a.account_name.lower(),
            'linkedin_total_impressions': lambda a: a.linkedin_total_impressions
        }.get(filters.sort_by, lambda a: a.pipeline_value)

        filtered = sorted(filtered, key=sort_key, reverse=reverse)

        # Paginate
        start = (filters.page - 1) * filters.page_size
        end = start + filters.page_size

        return filtered[start:end]

    def invalidate_cache(self):
        """Clear the cache"""
        self._cache.clear()
        self._cache_timestamp = None


# Singleton instance
_aggregator: Optional[ABMDataAggregator] = None


def get_aggregator() -> ABMDataAggregator:
    """Get or create aggregator singleton"""
    global _aggregator
    if _aggregator is None:
        _aggregator = ABMDataAggregator()
    return _aggregator
