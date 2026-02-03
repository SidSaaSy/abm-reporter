"""
ABM Reporter - CSV Upload Handler
Handles CSV imports from platforms without direct API access (e.g., Fibbler)
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from io import StringIO, BytesIO

logger = logging.getLogger(__name__)


class CSVHandler:
    """Handler for CSV data imports"""

    # Expected column mappings for different data sources
    FIBBLER_COLUMNS = {
        'company': 'account_name',
        'domain': 'domain',
        'linkedin_impressions': 'impressions',
        'linkedin_engagements': 'engagements',
        'linkedin_clicks': 'clicks',
        'content_type': 'content_type',
        'date': 'date'
    }

    LINKEDIN_EXPORT_COLUMNS = {
        'Company name': 'account_name',
        'Website': 'domain',
        'Impressions': 'impressions',
        'Clicks': 'clicks',
        'Engagement rate': 'engagement_rate',
        'Spend': 'spend'
    }

    def __init__(self):
        self._cached_data: Dict[str, pd.DataFrame] = {}

    def parse_fibbler_csv(self, csv_content: str | bytes) -> List[Dict[str, Any]]:
        """
        Parse Fibbler CSV export
        Fibbler provides LinkedIn engagement data by company
        """
        try:
            if isinstance(csv_content, bytes):
                df = pd.read_csv(BytesIO(csv_content))
            else:
                df = pd.read_csv(StringIO(csv_content))

            # Normalize column names
            df.columns = df.columns.str.lower().str.strip()

            # Try to map columns
            column_mapping = {}
            for orig, mapped in self.FIBBLER_COLUMNS.items():
                for col in df.columns:
                    if orig.lower() in col.lower():
                        column_mapping[col] = mapped
                        break

            if column_mapping:
                df = df.rename(columns=column_mapping)

            # Group by account/domain
            grouped_data = []
            if 'account_name' in df.columns or 'domain' in df.columns:
                group_col = 'account_name' if 'account_name' in df.columns else 'domain'

                for name, group in df.groupby(group_col):
                    record = {
                        'account_name': name,
                        'domain': group['domain'].iloc[0] if 'domain' in group.columns else None,
                        'impressions': group['impressions'].sum() if 'impressions' in group.columns else 0,
                        'engagements': group['engagements'].sum() if 'engagements' in group.columns else 0,
                        'clicks': group['clicks'].sum() if 'clicks' in group.columns else 0
                    }
                    grouped_data.append(record)
            else:
                # Return raw data if can't group
                grouped_data = df.to_dict('records')

            logger.info(f"Parsed {len(grouped_data)} accounts from Fibbler CSV")
            return grouped_data

        except Exception as e:
            logger.error(f"Error parsing Fibbler CSV: {e}")
            raise ValueError(f"Failed to parse Fibbler CSV: {e}")

    def parse_linkedin_ads_csv(self, csv_content: str | bytes) -> List[Dict[str, Any]]:
        """
        Parse LinkedIn Ads CSV export
        """
        try:
            if isinstance(csv_content, bytes):
                df = pd.read_csv(BytesIO(csv_content))
            else:
                df = pd.read_csv(StringIO(csv_content))

            # Map columns
            column_mapping = {}
            for orig, mapped in self.LINKEDIN_EXPORT_COLUMNS.items():
                if orig in df.columns:
                    column_mapping[orig] = mapped

            if column_mapping:
                df = df.rename(columns=column_mapping)

            # Clean numeric columns
            for col in ['impressions', 'clicks', 'spend']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

            # Clean percentage columns
            if 'engagement_rate' in df.columns:
                df['engagement_rate'] = pd.to_numeric(
                    df['engagement_rate'].astype(str).str.replace('%', ''),
                    errors='coerce'
                ).fillna(0)

            records = df.to_dict('records')
            logger.info(f"Parsed {len(records)} records from LinkedIn Ads CSV")
            return records

        except Exception as e:
            logger.error(f"Error parsing LinkedIn Ads CSV: {e}")
            raise ValueError(f"Failed to parse LinkedIn Ads CSV: {e}")

    def parse_generic_csv(self, csv_content: str | bytes) -> pd.DataFrame:
        """
        Parse generic CSV and return DataFrame
        Useful for custom data imports
        """
        try:
            if isinstance(csv_content, bytes):
                df = pd.read_csv(BytesIO(csv_content))
            else:
                df = pd.read_csv(StringIO(csv_content))

            logger.info(f"Parsed CSV with {len(df)} rows and columns: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            raise ValueError(f"Failed to parse CSV: {e}")

    def merge_csv_with_accounts(
            self,
            accounts: List[Dict[str, Any]],
            csv_data: List[Dict[str, Any]],
            match_field: str = 'domain'
    ) -> List[Dict[str, Any]]:
        """
        Merge CSV data with existing account data
        Matches on domain or account name
        """
        # Create lookup from CSV data
        csv_lookup = {}
        for record in csv_data:
            key = record.get(match_field, '').lower() if record.get(match_field) else None
            if key:
                csv_lookup[key] = record

        # Merge with accounts
        merged = []
        for account in accounts:
            account_key = None
            if match_field == 'domain':
                # Try matching any domain
                domains = account.get('domains', [])
                if isinstance(domains, str):
                    domains = [domains]
                for domain in domains:
                    if domain.lower() in csv_lookup:
                        account_key = domain.lower()
                        break
            else:
                account_key = account.get(match_field, '').lower()

            merged_account = account.copy()
            if account_key and account_key in csv_lookup:
                # Merge CSV data
                csv_record = csv_lookup[account_key]
                for key, value in csv_record.items():
                    if key not in merged_account or merged_account[key] is None:
                        merged_account[key] = value

            merged.append(merged_account)

        return merged

    def cache_data(self, key: str, data: pd.DataFrame):
        """Cache parsed data for later use"""
        self._cached_data[key] = data

    def get_cached_data(self, key: str) -> Optional[pd.DataFrame]:
        """Retrieve cached data"""
        return self._cached_data.get(key)

    def clear_cache(self, key: Optional[str] = None):
        """Clear cached data"""
        if key:
            self._cached_data.pop(key, None)
        else:
            self._cached_data.clear()


# Singleton instance
_csv_handler: Optional[CSVHandler] = None


def get_csv_handler() -> CSVHandler:
    """Get or create CSV handler singleton"""
    global _csv_handler
    if _csv_handler is None:
        _csv_handler = CSVHandler()
    return _csv_handler
