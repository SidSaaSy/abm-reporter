"""
ABM Reporter - Salesforce Integration
Handles all Salesforce data fetching for accounts, contacts, and opportunities
"""
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

from ..config import get_settings
from ..models.account import Contact, Opportunity

logger = logging.getLogger(__name__)


class SalesforceClient:
    """Salesforce API client for ABM data"""

    def __init__(self):
        self.settings = get_settings()
        self._client: Optional[Salesforce] = None

    def _get_client(self) -> Salesforce:
        """Get or create Salesforce client"""
        if self._client is None:
            try:
                self._client = Salesforce(
                    username=self.settings.SFDC_USERNAME,
                    password=self.settings.SFDC_PASSWORD,
                    security_token=self.settings.SFDC_SECURITY_TOKEN,
                    domain=self.settings.SFDC_DOMAIN
                )
                logger.info("Successfully connected to Salesforce")
            except SalesforceError as e:
                logger.error(f"Failed to connect to Salesforce: {e}")
                raise
        return self._client

    async def get_accounts(self, domains: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fetch accounts from Salesforce
        Optionally filter by website domains
        """
        sf = self._get_client()

        query = """
            SELECT Id, Name, Website, Industry, NumberOfEmployees,
                   AnnualRevenue, BillingCity, BillingCountry, Type,
                   CreatedDate, LastModifiedDate
            FROM Account
            WHERE IsDeleted = false
        """

        if domains:
            # Build domain filter
            domain_conditions = " OR ".join([f"Website LIKE '%{d}%'" for d in domains])
            query += f" AND ({domain_conditions})"

        query += " ORDER BY Name"

        try:
            result = sf.query_all(query)
            accounts = result.get('records', [])
            logger.info(f"Fetched {len(accounts)} accounts from Salesforce")
            return accounts
        except SalesforceError as e:
            logger.error(f"Error fetching accounts: {e}")
            raise

    async def get_contacts_by_account(self, account_id: str) -> List[Contact]:
        """Fetch all contacts for a specific account"""
        sf = self._get_client()

        query = f"""
            SELECT Id, Email, FirstName, LastName, Title, AccountId, CreatedDate
            FROM Contact
            WHERE AccountId = '{account_id}'
            AND IsDeleted = false
        """

        try:
            result = sf.query_all(query)
            contacts = [
                Contact(
                    id=record['Id'],
                    email=record.get('Email'),
                    first_name=record.get('FirstName'),
                    last_name=record.get('LastName'),
                    title=record.get('Title'),
                    source='sfdc',
                    account_id=record.get('AccountId'),
                    created_at=datetime.fromisoformat(record['CreatedDate'].replace('Z', '+00:00'))
                    if record.get('CreatedDate') else None
                )
                for record in result.get('records', [])
            ]
            return contacts
        except SalesforceError as e:
            logger.error(f"Error fetching contacts for account {account_id}: {e}")
            raise

        async def get_contacts_count_by_account(self) -> Dict[str, int]:
        """Get contact counts grouped by account"""
        sf = self._get_client()
        # Use query() not query_all() - aggregate queries don't support pagination
        # LIMIT 2000 is Salesforce's max for aggregate queries
        query = """
            SELECT AccountId, COUNT(Id) contactCount
            FROM Contact
            WHERE AccountId != null AND IsDeleted = false
            GROUP BY AccountId
            LIMIT 2000
        """
        try:
            result = sf.query(query)
            return {
                record['AccountId']: record['contactCount']
                for record in result.get('records', [])
            }
        except SalesforceError as e:
            logger.error(f"Error fetching contact counts: {e}")
            # Return empty dict instead of failing - graceful degradation
            return {}
    
    async def get_opportunities_by_account(self, account_id: str) -> List[Opportunity]:
        """Fetch all opportunities for a specific account"""
        sf = self._get_client()

        query = f"""
            SELECT Id, Name, Amount, StageName, CloseDate,
                   IsWon, IsClosed, AccountId, CreatedDate
            FROM Opportunity
            WHERE AccountId = '{account_id}'
            AND IsDeleted = false
        """

        try:
            result = sf.query_all(query)
            opportunities = [
                Opportunity(
                    id=record['Id'],
                    name=record['Name'],
                    amount=record.get('Amount'),
                    stage=record['StageName'],
                    close_date=datetime.fromisoformat(record['CloseDate'])
                    if record.get('CloseDate') else None,
                    is_won=record.get('IsWon', False),
                    is_closed=record.get('IsClosed', False),
                    account_id=record['AccountId'],
                    created_at=datetime.fromisoformat(record['CreatedDate'].replace('Z', '+00:00'))
                    if record.get('CreatedDate') else None
                )
                for record in result.get('records', [])
            ]
            return opportunities
        except SalesforceError as e:
            logger.error(f"Error fetching opportunities for account {account_id}: {e}")
            raise

    async def get_opportunity_summary(self) -> Dict[str, Dict[str, Any]]:
        """
        Get opportunity summary grouped by account
        Returns: {account_id: {open_opps, closed_won, closed_lost, pipeline_value}}
        """
        sf = self._get_client()

        # Get open opportunities
        open_query = """
            SELECT AccountId, COUNT(Id) oppCount, SUM(Amount) totalAmount
            FROM Opportunity
            WHERE IsClosed = false AND AccountId != null
            GROUP BY AccountId
        """

        # Get closed won
        won_query = """
            SELECT AccountId, COUNT(Id) oppCount, SUM(Amount) totalAmount
            FROM Opportunity
            WHERE IsWon = true AND AccountId != null
            GROUP BY AccountId
        """

        # Get closed lost
        lost_query = """
            SELECT AccountId, COUNT(Id) oppCount
            FROM Opportunity
            WHERE IsClosed = true AND IsWon = false AND AccountId != null
            GROUP BY AccountId
        """

        try:
            open_result = sf.query_all(open_query)
            won_result = sf.query_all(won_query)
            lost_result = sf.query_all(lost_query)

            summary = {}

            # Process open opportunities
            for record in open_result.get('records', []):
                account_id = record['AccountId']
                summary[account_id] = {
                    'open_opps': record['oppCount'],
                    'pipeline_value': record['totalAmount'] or 0,
                    'closed_won': 0,
                    'closed_lost': 0
                }

            # Process closed won
            for record in won_result.get('records', []):
                account_id = record['AccountId']
                if account_id not in summary:
                    summary[account_id] = {'open_opps': 0, 'pipeline_value': 0, 'closed_won': 0, 'closed_lost': 0}
                summary[account_id]['closed_won'] = record['oppCount']

            # Process closed lost
            for record in lost_result.get('records', []):
                account_id = record['AccountId']
                if account_id not in summary:
                    summary[account_id] = {'open_opps': 0, 'pipeline_value': 0, 'closed_won': 0, 'closed_lost': 0}
                summary[account_id]['closed_lost'] = record['oppCount']

            return summary
        except SalesforceError as e:
            logger.error(f"Error fetching opportunity summary: {e}")
            raise

    async def search_accounts_by_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Search for accounts by website domain"""
        sf = self._get_client()

        query = f"""
            SELECT Id, Name, Website, Industry, NumberOfEmployees, AnnualRevenue
            FROM Account
            WHERE Website LIKE '%{domain}%'
            AND IsDeleted = false
        """

        try:
            result = sf.query_all(query)
            return result.get('records', [])
        except SalesforceError as e:
            logger.error(f"Error searching accounts by domain {domain}: {e}")
            raise

    async def get_recently_modified_accounts(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get accounts modified in the last N days"""
        sf = self._get_client()

        cutoff_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')

        query = f"""
            SELECT Id, Name, Website, Industry, LastModifiedDate
            FROM Account
            WHERE LastModifiedDate >= {cutoff_date}
            AND IsDeleted = false
            ORDER BY LastModifiedDate DESC
        """

        try:
            result = sf.query_all(query)
            return result.get('records', [])
        except SalesforceError as e:
            logger.error(f"Error fetching recently modified accounts: {e}")
            raise


# Singleton instance
_salesforce_client: Optional[SalesforceClient] = None


def get_salesforce_client() -> SalesforceClient:
    """Get or create Salesforce client singleton"""
    global _salesforce_client
    if _salesforce_client is None:
        _salesforce_client = SalesforceClient()
    return _salesforce_client
