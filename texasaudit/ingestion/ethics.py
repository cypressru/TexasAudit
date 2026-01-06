"""
Texas Ethics Commission campaign finance data ingestion.

Downloads bulk campaign finance data from the Texas Ethics Commission
and imports contribution records into the database.

Source: https://www.ethics.state.tx.us/search/cf/
Bulk Data: https://www.ethics.state.tx.us/search/cf/TEC_CF_CSV.zip
"""

import csv
import io
import zipfile
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Optional

import requests
from tqdm import tqdm

from texasaudit.config import config
from texasaudit.database import get_session, CampaignContribution
from texasaudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class EthicsIngestor(BaseIngestor):
    """Ingestor for Texas Ethics Commission campaign finance data."""

    source_name = "campaign_finance"

    # TEC bulk data download URL (new location as of 2025)
    TEC_URL = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip"

    # Minimum contribution amount to import (focus on significant contributions)
    MIN_AMOUNT = Decimal("500.00")

    def __init__(self):
        super().__init__()
        self.data_dir = config.data_dir

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Download and import TEC campaign finance data.

        Note: TEC bulk downloads don't support incremental sync by timestamp,
        so we always do a full download but only import contributions >= $500.
        """
        print("Downloading TEC campaign finance data...")

        # Download the ZIP file
        try:
            response = requests.get(self.TEC_URL, timeout=300, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))

            # Download with progress bar
            zip_data = io.BytesIO()
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    zip_data.write(chunk)
                    pbar.update(len(chunk))

            zip_data.seek(0)

        except Exception as e:
            raise Exception(f"Failed to download TEC data: {e}")

        # Process the ZIP file
        print("Processing TEC data files...")
        total_records = 0

        try:
            with zipfile.ZipFile(zip_data, 'r') as zf:
                # List all files in the ZIP
                file_list = zf.namelist()
                print(f"  Found {len(file_list)} files in archive")

                # Look for contribution-related files
                # Based on TEC CSV format, common files include:
                # - contribs_*.csv (contribution records)
                # - Any file with "contrib" or "contribution" in the name
                contrib_files = [
                    f for f in file_list
                    if 'contrib' in f.lower() and f.lower().endswith('.csv')
                ]

                if not contrib_files:
                    # If no specific contrib files, try all CSV files
                    print("  No contribution-specific files found, checking all CSV files...")
                    contrib_files = [f for f in file_list if f.lower().endswith('.csv')]

                print(f"  Processing {len(contrib_files)} contribution files...")

                for filename in contrib_files:
                    print(f"  Processing {filename}...")
                    try:
                        # Read CSV from ZIP
                        with zf.open(filename) as csv_file:
                            # Decode content (TEC files are typically Latin-1)
                            content = csv_file.read().decode('latin-1', errors='replace')
                            count = self._import_contributions(content, filename)
                            total_records += count
                            print(f"    Imported {count:,} contributions from {filename}")
                    except Exception as e:
                        print(f"    Error processing {filename}: {e}")
                        continue

        except zipfile.BadZipFile as e:
            raise Exception(f"Invalid ZIP file: {e}")

        return total_records

    def _import_contributions(self, csv_content: str, source_file: str) -> int:
        """
        Import contributions from CSV content.

        Args:
            csv_content: CSV file content as string
            source_file: Name of the source file for tracking

        Returns:
            Number of records imported
        """
        count = 0

        try:
            reader = csv.DictReader(io.StringIO(csv_content))

            # Get all rows first to show progress
            rows = list(reader)
            if not rows:
                return 0

            # Filter for contributions >= $500
            significant_rows = []
            for row in rows:
                amount = self._parse_amount(row)
                if amount and amount >= self.MIN_AMOUNT:
                    significant_rows.append(row)

            if not significant_rows:
                print(f"      No contributions >= ${self.MIN_AMOUNT} found")
                return 0

            print(f"      Found {len(significant_rows):,} contributions >= ${self.MIN_AMOUNT}")

            # Process in batches
            batch_size = 1000
            with get_session() as session:
                for i in tqdm(range(0, len(significant_rows), batch_size),
                             desc=f"      Importing", leave=False):
                    batch = significant_rows[i:i + batch_size]

                    for row in batch:
                        contribution = self._create_contribution(session, row, source_file)
                        if contribution:
                            count += 1

                    # Commit batch
                    session.commit()

        except Exception as e:
            print(f"      Error importing contributions: {e}")
            raise

        return count

    def _create_contribution(self, session, row: dict, source_file: str) -> Optional[CampaignContribution]:
        """
        Create a CampaignContribution record from CSV row.

        TEC CSV field names can vary, but common patterns include:
        - Filer: filerIdent, filerName, filerType
        - Contributor: contributorNameOrganization, contributorNameFirst, contributorNameLast
        - Amount: contributionAmount, amount
        - Date: contributionDate, receivedDate
        - Location: contributorCity, contributorState
        - Employer: contributorEmployer
        """
        # Parse amount
        amount = self._parse_amount(row)
        if not amount or amount < self.MIN_AMOUNT:
            return None

        # Get filer information (the recipient/candidate)
        filer_name = (
            row.get('filerName', '') or
            row.get('candidateName', '') or
            row.get('committeeNameOrAccount', '') or
            row.get('recipientName', '') or
            ''
        ).strip()

        if not filer_name:
            return None

        filer_type = (
            row.get('filerType', '') or
            row.get('filerTypeCd', '') or
            row.get('recipientType', '') or
            ''
        ).strip()

        # Get contributor information
        # Contributors may have separate first/last name fields or a single name field
        contributor_first = row.get('contributorNameFirst', '').strip()
        contributor_last = row.get('contributorNameLast', '').strip()
        contributor_org = row.get('contributorNameOrganization', '').strip()

        if contributor_org:
            contributor_name = contributor_org
        elif contributor_first or contributor_last:
            contributor_name = f"{contributor_first} {contributor_last}".strip()
        else:
            # Try other common field names
            contributor_name = (
                row.get('contributorName', '') or
                row.get('contributor', '') or
                row.get('payorName', '') or
                ''
            ).strip()

        if not contributor_name:
            return None

        # Normalize contributor name for matching
        contributor_normalized = normalize_vendor_name(contributor_name)

        # Get contributor type
        contributor_type = (
            row.get('contributorType', '') or
            row.get('contributorTypeCd', '') or
            row.get('entityType', '') or
            ''
        ).strip()

        # Parse date
        contribution_date = self._parse_date(row)

        # Get location information
        contributor_city = (
            row.get('contributorCity', '') or
            row.get('city', '') or
            ''
        ).strip()

        contributor_state = (
            row.get('contributorState', '') or
            row.get('contributorStateCd', '') or
            row.get('state', '') or
            ''
        ).strip()

        # Get employer
        contributor_employer = (
            row.get('contributorEmployer', '') or
            row.get('employerName', '') or
            ''
        ).strip()

        # Create unique source ID for deduplication
        # Use a combination of filer, contributor, amount, and date
        source_id = (
            f"{row.get('recordId', '')}_{row.get('filerIdent', '')}_{filer_name}_"
            f"{contributor_name}_{amount}_{contribution_date}_{source_file}"
        )

        # Check for duplicate
        existing = session.query(CampaignContribution).filter(
            CampaignContribution.filer_name == filer_name,
            CampaignContribution.contributor_name == contributor_name,
            CampaignContribution.contribution_amount == amount,
            CampaignContribution.contribution_date == contribution_date,
        ).first()

        if existing:
            return None

        # Create contribution record
        contribution = CampaignContribution(
            filer_name=filer_name,
            filer_type=filer_type if filer_type else None,
            contributor_name=contributor_name,
            contributor_normalized=contributor_normalized,
            contributor_type=contributor_type if contributor_type else None,
            contribution_amount=amount,
            contribution_date=contribution_date,
            contributor_city=contributor_city if contributor_city else None,
            contributor_state=contributor_state if contributor_state else None,
            contributor_employer=contributor_employer if contributor_employer else None,
            raw_data=dict(row),
        )

        session.add(contribution)
        return contribution

    def _parse_amount(self, row: dict) -> Optional[Decimal]:
        """Parse contribution amount from row."""
        amount_str = (
            row.get('contributionAmount', '') or
            row.get('amount', '') or
            row.get('contributionInfoAmount', '') or
            ''
        ).strip()

        if not amount_str:
            return None

        try:
            # Remove currency symbols and commas
            amount_str = amount_str.replace('$', '').replace(',', '').strip()
            return Decimal(amount_str)
        except (InvalidOperation, ValueError):
            return None

    def _parse_date(self, row: dict) -> Optional[date]:
        """Parse contribution date from row."""
        date_str = (
            row.get('contributionDate', '') or
            row.get('receivedDate', '') or
            row.get('contributionInfoDate', '') or
            row.get('date', '') or
            ''
        ).strip()

        if not date_str:
            return None

        # Try common date formats
        for fmt in [
            '%Y%m%d',           # YYYYMMDD
            '%m/%d/%Y',         # MM/DD/YYYY
            '%Y-%m-%d',         # YYYY-MM-DD
            '%m-%d-%Y',         # MM-DD-YYYY
            '%Y/%m/%d',         # YYYY/MM/DD
        ]:
            try:
                return datetime.strptime(date_str[:10], fmt).date()
            except ValueError:
                continue

        return None


# Helper functions for querying TEC data

def search_contributions_by_contributor(contributor_name: str, min_amount: Decimal = None) -> list:
    """
    Search campaign contributions by contributor name.

    Args:
        contributor_name: Name to search for (uses normalized matching)
        min_amount: Optional minimum contribution amount

    Returns:
        List of CampaignContribution records
    """
    from texasaudit.database import get_session

    normalized = normalize_vendor_name(contributor_name)

    with get_session() as session:
        query = session.query(CampaignContribution).filter(
            CampaignContribution.contributor_normalized.ilike(f'%{normalized}%')
        )

        if min_amount:
            query = query.filter(CampaignContribution.contribution_amount >= min_amount)

        return query.order_by(CampaignContribution.contribution_amount.desc()).all()


def search_contributions_by_filer(filer_name: str, min_amount: Decimal = None) -> list:
    """
    Search campaign contributions by filer/recipient name.

    Args:
        filer_name: Filer name to search for
        min_amount: Optional minimum contribution amount

    Returns:
        List of CampaignContribution records
    """
    from texasaudit.database import get_session

    with get_session() as session:
        query = session.query(CampaignContribution).filter(
            CampaignContribution.filer_name.ilike(f'%{filer_name}%')
        )

        if min_amount:
            query = query.filter(CampaignContribution.contribution_amount >= min_amount)

        return query.order_by(CampaignContribution.contribution_amount.desc()).all()


def get_top_contributors(limit: int = 100, min_amount: Decimal = None) -> list:
    """
    Get top contributors by total contribution amount.

    Args:
        limit: Number of top contributors to return
        min_amount: Optional minimum contribution amount

    Returns:
        List of dicts with contributor info and total amounts
    """
    from texasaudit.database import get_session
    from sqlalchemy import func

    with get_session() as session:
        query = session.query(
            CampaignContribution.contributor_normalized,
            CampaignContribution.contributor_name,
            func.sum(CampaignContribution.contribution_amount).label('total_amount'),
            func.count(CampaignContribution.id).label('contribution_count'),
        ).group_by(
            CampaignContribution.contributor_normalized,
            CampaignContribution.contributor_name,
        )

        if min_amount:
            query = query.filter(CampaignContribution.contribution_amount >= min_amount)

        results = query.order_by(func.sum(CampaignContribution.contribution_amount).desc()).limit(limit).all()

        return [
            {
                'contributor_normalized': r[0],
                'contributor_name': r[1],
                'total_amount': r[2],
                'contribution_count': r[3],
            }
            for r in results
        ]
