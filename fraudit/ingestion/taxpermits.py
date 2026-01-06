"""
Tax permit data ingestor for Texas.

Fetches active franchise tax and sales tax permit holders from data.texas.gov
via the Socrata API.
"""

from datetime import datetime, date
from typing import Optional

from sodapy import Socrata
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, TaxPermit
from fraudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class TaxPermitsIngestor(BaseIngestor):
    """Ingestor for Texas tax permit data from data.texas.gov."""

    source_name = "tax_permits"

    # Socrata domain and dataset IDs
    DOMAIN = "data.texas.gov"
    DATASETS = {
        "franchise_tax": "9cir-efmm",  # Active Franchise Tax Permit Holders
        "sales_tax": "jrea-zgmq",      # Active Sales Tax Permit Holders
    }

    # Maximum records per API request
    PAGE_SIZE = 50000

    def __init__(self):
        super().__init__()
        self.client = None

    def _get_client(self) -> Socrata:
        """Get or create Socrata client."""
        if self.client is None:
            app_token = config.socrata_token
            self.client = Socrata(
                self.DOMAIN,
                app_token=app_token,
                timeout=120,
            )
        return self.client

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Sync tax permit data from Socrata.

        Args:
            since: Only fetch records updated after this timestamp (not used for tax permits).

        Returns:
            Total number of records synced.
        """
        print("Syncing tax permit data from data.texas.gov...")

        total_synced = 0

        # Sync franchise tax permits
        print("\n  Downloading franchise tax permits...")
        franchise_count = self._sync_permits("franchise_tax", "franchise")
        print(f"  Franchise tax permits: {franchise_count:,} records")
        total_synced += franchise_count

        # Sync sales tax permits
        print("\n  Downloading sales tax permits...")
        sales_count = self._sync_permits("sales_tax", "sales")
        print(f"  Sales tax permits: {sales_count:,} records")
        total_synced += sales_count

        print(f"\nTotal tax permits synced: {total_synced:,}")
        return total_synced

    def _sync_permits(self, dataset_key: str, permit_type: str) -> int:
        """
        Sync tax permits for a specific permit type.

        Args:
            dataset_key: Key in DATASETS dict
            permit_type: Type of permit (franchise or sales)

        Returns:
            Number of records synced.
        """
        client = self._get_client()
        dataset_id = self.DATASETS[dataset_key]

        try:
            # Get total count first
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"])

            if total_count == 0:
                return 0

            # Paginate through results
            offset = 0
            records_processed = 0

            with tqdm(total=total_count, desc=f"  {permit_type.capitalize()}", leave=False) as pbar:
                while offset < total_count:
                    results = client.get(
                        dataset_id,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                    )

                    if not results:
                        break

                    # Process batch
                    processed = self._process_permit_batch(results, permit_type)
                    records_processed += processed

                    offset += len(results)
                    pbar.update(len(results))

            return records_processed

        except Exception as e:
            print(f"  Error syncing {permit_type} permits: {e}")
            return 0

    def _process_permit_batch(self, records: list[dict], permit_type: str) -> int:
        """
        Process a batch of tax permit records.

        Args:
            records: List of permit records from Socrata
            permit_type: Type of permit (franchise or sales)

        Returns:
            Number of records created.
        """
        count = 0

        with get_session() as session:
            for record in records:
                permit = self._create_permit(session, record, permit_type)
                if permit:
                    count += 1

        return count

    def _create_permit(self, session, record: dict, permit_type: str) -> Optional[TaxPermit]:
        """
        Create a tax permit record from Socrata data.

        Args:
            session: Database session
            record: Raw record from Socrata API
            permit_type: Type of permit (franchise or sales)

        Returns:
            TaxPermit instance if created, None otherwise.
        """
        # Extract taxpayer name (required)
        taxpayer_name = record.get("taxpayer_name", "")
        if not taxpayer_name:
            return None

        # Normalize taxpayer name for matching
        taxpayer_normalized = normalize_vendor_name(taxpayer_name)

        # Extract taxpayer number
        taxpayer_number = record.get("taxpayer_number", record.get("taxpayer_no", ""))

        # Check for duplicate based on taxpayer_number and permit_type
        if taxpayer_number:
            existing = session.query(TaxPermit).filter(
                TaxPermit.taxpayer_number == taxpayer_number,
                TaxPermit.permit_type == permit_type,
            ).first()
            if existing:
                # Update existing record
                existing.taxpayer_name = taxpayer_name
                existing.taxpayer_normalized = taxpayer_normalized
                existing.business_name = record.get("business_name", "")
                existing.business_address = record.get("physical_address", record.get("address", ""))
                existing.business_city = record.get("physical_city", record.get("city", ""))
                existing.business_state = record.get("physical_state", record.get("state", ""))
                existing.business_zip = record.get("physical_zip", record.get("zip", ""))
                existing.naics_code = record.get("naics_code", record.get("naics", ""))
                existing.sic_code = record.get("sic_code", record.get("sic", ""))
                existing.permit_status = record.get("permit_status", record.get("status", "active"))
                existing.first_sale_date = self._parse_date(record.get("first_sale_date", record.get("first_sales_date", "")))
                existing.raw_data = record
                return existing  # Count as updated

        # Parse first sale date if available
        first_sale_date = self._parse_date(record.get("first_sale_date", record.get("first_sales_date", "")))

        # Create new permit record
        permit = TaxPermit(
            permit_type=permit_type,
            taxpayer_name=taxpayer_name,
            taxpayer_normalized=taxpayer_normalized,
            taxpayer_number=taxpayer_number if taxpayer_number else None,
            business_name=record.get("business_name", ""),
            business_address=record.get("physical_address", record.get("address", "")),
            business_city=record.get("physical_city", record.get("city", "")),
            business_state=record.get("physical_state", record.get("state", "")),
            business_zip=record.get("physical_zip", record.get("zip", "")),
            naics_code=record.get("naics_code", record.get("naics", "")),
            sic_code=record.get("sic_code", record.get("sic", "")),
            permit_status=record.get("permit_status", record.get("status", "active")),
            first_sale_date=first_sale_date,
            raw_data=record,
        )
        session.add(permit)

        return permit

    def _parse_date(self, date_str: str) -> Optional[date]:
        """
        Parse a date string from various formats.

        Args:
            date_str: Date string to parse

        Returns:
            date object or None if parsing fails
        """
        if not date_str:
            return None

        try:
            # Handle various date formats
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(date_str[:19], fmt).date()
                except ValueError:
                    continue
        except Exception:
            pass

        return None
