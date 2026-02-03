"""
ABM Reporter - HubSpot Integration
Handles all HubSpot data fetching for contacts and form submissions
"""
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import httpx

from ..config import get_settings
from ..models.account import Contact, FormSubmission

logger = logging.getLogger(__name__)


class HubSpotClient:
    """HubSpot API client for ABM data"""

    BASE_URL = "https://api.hubapi.com"

    def __init__(self):
        self.settings = get_settings()
        self._access_token = self.settings.HUBSPOT_ACCESS_TOKEN

    def _get_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    async def _make_request(
            self,
            method: str,
            endpoint: str,
            params: Optional[Dict] = None,
            json_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to HubSpot API"""
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
                logger.error(f"HubSpot API error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"HubSpot request error: {e}")
                raise

    async def get_companies(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch companies from HubSpot"""
        endpoint = "/crm/v3/objects/companies"
        params = {
            "limit": limit,
            "properties": "name,domain,industry,numberofemployees,annualrevenue,city,country"
        }

        try:
            result = await self._make_request("GET", endpoint, params=params)
            companies = result.get('results', [])
            logger.info(f"Fetched {len(companies)} companies from HubSpot")
            return companies
        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            raise

    async def get_contacts_by_company(self, company_id: str) -> List[Contact]:
        """Fetch contacts associated with a company"""
        # First get contact associations
        assoc_endpoint = f"/crm/v3/objects/companies/{company_id}/associations/contacts"

        try:
            assoc_result = await self._make_request("GET", assoc_endpoint)
            contact_ids = [assoc['id'] for assoc in assoc_result.get('results', [])]

            if not contact_ids:
                return []

            # Batch fetch contact details
            contacts = []
            for contact_id in contact_ids:
                endpoint = f"/crm/v3/objects/contacts/{contact_id}"
                params = {"properties": "email,firstname,lastname,jobtitle,createdate"}

                contact_data = await self._make_request("GET", endpoint, params=params)
                props = contact_data.get('properties', {})

                contacts.append(Contact(
                    id=contact_data['id'],
                    email=props.get('email'),
                    first_name=props.get('firstname'),
                    last_name=props.get('lastname'),
                    title=props.get('jobtitle'),
                    source='hubspot',
                    account_id=company_id,
                    created_at=datetime.fromisoformat(props['createdate'].replace('Z', '+00:00'))
                    if props.get('createdate') else None
                ))

            return contacts
        except Exception as e:
            logger.error(f"Error fetching contacts for company {company_id}: {e}")
            raise

    async def get_contacts_count_by_company_domain(self) -> Dict[str, int]:
        """
        Get contact counts grouped by company domain
        Returns: {domain: contact_count}
        """
        endpoint = "/crm/v3/objects/contacts/search"

        # Search for all contacts with company association
        search_body = {
            "filterGroups": [],
            "properties": ["email", "associatedcompanyid"],
            "limit": 100
        }

        try:
            all_contacts = []
            after = None

            while True:
                if after:
                    search_body["after"] = after

                result = await self._make_request("POST", endpoint, json_data=search_body)
                contacts = result.get('results', [])
                all_contacts.extend(contacts)

                # Check for pagination
                paging = result.get('paging', {})
                next_page = paging.get('next', {})
                after = next_page.get('after')

                if not after or len(contacts) < 100:
                    break

            # Group by company
            company_counts: Dict[str, int] = {}
            for contact in all_contacts:
                company_id = contact.get('properties', {}).get('associatedcompanyid')
                if company_id:
                    company_counts[company_id] = company_counts.get(company_id, 0) + 1

            logger.info(f"Found contacts for {len(company_counts)} companies")
            return company_counts
        except Exception as e:
            logger.error(f"Error fetching contact counts: {e}")
            raise

    async def get_form_submissions(
            self,
            form_id: Optional[str] = None,
            since: Optional[datetime] = None
    ) -> List[FormSubmission]:
        """Fetch form submissions, optionally filtered by form and date"""
        endpoint = "/form-integrations/v1/submissions/forms"

        if form_id:
            endpoint = f"/form-integrations/v1/submissions/forms/{form_id}"

        try:
            result = await self._make_request("GET", endpoint)
            submissions = []

            for record in result.get('results', []):
                submitted_at = datetime.fromtimestamp(record['submittedAt'] / 1000)

                if since and submitted_at < since:
                    continue

                # Extract email from form values
                email = None
                for field in record.get('values', []):
                    if field.get('name') == 'email':
                        email = field.get('value')
                        break

                submissions.append(FormSubmission(
                    id=record.get('conversionId', ''),
                    form_name=record.get('formId', 'Unknown'),
                    submitted_at=submitted_at,
                    contact_email=email,
                    page_url=record.get('pageUrl')
                ))

            logger.info(f"Fetched {len(submissions)} form submissions")
            return submissions
        except Exception as e:
            logger.error(f"Error fetching form submissions: {e}")
            raise

    async def get_form_submissions_by_company(self, company_domain: str) -> List[FormSubmission]:
        """Get form submissions from contacts at a specific company domain"""
        # This requires matching email domains to company domains
        all_submissions = await self.get_form_submissions()

        # Filter submissions by email domain
        matched_submissions = []
        for submission in all_submissions:
            if submission.contact_email:
                email_domain = submission.contact_email.split('@')[-1]
                if company_domain.lower() in email_domain.lower():
                    matched_submissions.append(submission)

        return matched_submissions

    async def get_forms(self) -> List[Dict[str, Any]]:
        """Get list of all forms"""
        endpoint = "/marketing/v3/forms"

        try:
            result = await self._make_request("GET", endpoint)
            forms = result.get('results', [])
            logger.info(f"Fetched {len(forms)} forms")
            return forms
        except Exception as e:
            logger.error(f"Error fetching forms: {e}")
            raise

    async def search_contacts_by_email_domain(self, domain: str) -> List[Contact]:
        """Search for contacts by email domain"""
        endpoint = "/crm/v3/objects/contacts/search"

        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "CONTAINS_TOKEN",
                    "value": f"@{domain}"
                }]
            }],
            "properties": ["email", "firstname", "lastname", "jobtitle", "createdate", "associatedcompanyid"],
            "limit": 100
        }

        try:
            result = await self._make_request("POST", endpoint, json_data=search_body)

            contacts = [
                Contact(
                    id=record['id'],
                    email=record['properties'].get('email'),
                    first_name=record['properties'].get('firstname'),
                    last_name=record['properties'].get('lastname'),
                    title=record['properties'].get('jobtitle'),
                    source='hubspot',
                    account_id=record['properties'].get('associatedcompanyid'),
                    created_at=datetime.fromisoformat(
                        record['properties']['createdate'].replace('Z', '+00:00')
                    ) if record['properties'].get('createdate') else None
                )
                for record in result.get('results', [])
            ]

            logger.info(f"Found {len(contacts)} contacts for domain {domain}")
            return contacts
        except Exception as e:
            logger.error(f"Error searching contacts by domain: {e}")
            raise


# Singleton instance
_hubspot_client: Optional[HubSpotClient] = None


def get_hubspot_client() -> HubSpotClient:
    """Get or create HubSpot client singleton"""
    global _hubspot_client
    if _hubspot_client is None:
        _hubspot_client = HubSpotClient()
    return _hubspot_client
