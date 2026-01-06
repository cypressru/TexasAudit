"""
Socrata API client for data.texas.gov.

Handles payments, tax permits, and other datasets available via the SODA API.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from decimal import Decimal
from threading import Lock
from typing import Optional

from sodapy import Socrata
from tqdm import tqdm

from texasaudit.config import config
from texasaudit.database import get_session, Payment, Vendor, Agency
from texasaudit.normalization import (
    normalize_fiscal_years,
    normalize_vendor_name,
)
from .base import BaseIngestor


class SocrataIngestor(BaseIngestor):
    """Ingestor for data.texas.gov Socrata datasets."""

    source_name = "socrata_payments"

    # Socrata domain and dataset IDs
    DOMAIN = "data.texas.gov"

    # Key datasets - Expenditures by county/agency by fiscal year
    DATASETS = {
        # Expenditures by fiscal year (aggregate by agency/county)
        "expenditures_2024": "2zpi-yjjs",
        "expenditures_2023": "iyey-5sid",
        "expenditures_2022": "xys8-xb33",
        "expenditures_2021": "tup7-smjg",
        "expenditures_2020": "aact-g69n",
        "expenditures_2019": "2x5x-m677",
        "expenditures_2018": "f2iw-dtqt",

        # Tax data (for vendor verification)
        "franchise_tax_permits": "9cir-efmm",
        "sales_tax_permits": "jrea-zgmq",

        # Other useful datasets
        "mixed_beverage_receipts": "naix-2893",
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
        Sync expenditure data from Socrata.

        Args:
            since: Only fetch records updated after this timestamp.
        """
        print("Syncing expenditure data from data.texas.gov...")

        # Build list of years to sync
        years_to_sync = []
        for year in range(2024, 2017, -1):
            dataset_key = f"expenditures_{year}"
            if dataset_key in self.DATASETS:
                years_to_sync.append((year, self.DATASETS[dataset_key]))

        if not years_to_sync:
            return 0

        # Progress tracking
        print(f"  Downloading {len(years_to_sync)} fiscal years in parallel...")
        results = {}
        total = 0

        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=len(years_to_sync)) as executor:
            # Submit all years
            future_to_year = {
                executor.submit(self._sync_expenditures_threaded, year, dataset_id): year
                for year, dataset_id in years_to_sync
            }

            # Process as they complete
            for future in as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    count = future.result()
                    results[year] = count
                    total += count
                    print(f"  ✓ FY{year}: {count:,} records")
                except Exception as e:
                    print(f"  ✗ FY{year}: {e}")
                    results[year] = 0

        return total

    def _sync_expenditures_threaded(self, year: int, dataset_id: str) -> int:
        """Thread-safe version of expenditure sync for a single year."""
        # Each thread gets its own Socrata client
        client = Socrata(
            self.DOMAIN,
            app_token=config.socrata_token,
            timeout=120,
        )

        try:
            # Get total count first
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"])

            if total_count == 0:
                return 0

            # Paginate through results
            offset = 0
            records_processed = 0

            while offset < total_count:
                results = client.get(
                    dataset_id,
                    limit=self.PAGE_SIZE,
                    offset=offset,
                )

                if not results:
                    break

                # Process batch
                processed = self._process_expenditure_batch(results, year)
                records_processed += processed
                offset += len(results)

            return records_processed
        except Exception as e:
            raise Exception(f"Error syncing FY{year}: {e}")

    def _sync_expenditures(self, year: int, dataset_id: str) -> int:
        """Sync expenditures for a specific fiscal year."""
        client = self._get_client()

        try:
            # Get total count first
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"])
            print(f"  FY{year}: {total_count:,} records")

            if total_count == 0:
                return 0

            # Paginate through results
            offset = 0
            records_processed = 0

            with tqdm(total=total_count, desc=f"FY{year}", leave=False) as pbar:
                while offset < total_count:
                    results = client.get(
                        dataset_id,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                    )

                    if not results:
                        break

                    # Process batch
                    processed = self._process_expenditure_batch(results, year)
                    records_processed += processed

                    offset += len(results)
                    pbar.update(len(results))

            return records_processed
        except Exception as e:
            print(f"  FY{year}: Error - {e}")
            return 0

    def _process_expenditure_batch(self, records: list[dict], fiscal_year: int) -> int:
        """Process a batch of expenditure records (agency-level aggregates)."""
        count = 0

        with get_session() as session:
            for record in records:
                # Extract amount
                amount_str = record.get("amount", "0")
                try:
                    amount = Decimal(str(amount_str).replace(",", "").replace("$", ""))
                except:
                    continue

                if amount == 0:
                    continue

                # Get or create agency
                agency_name = record.get("agency_name", "")
                agency_code = record.get("agency_number", "")
                if not agency_name and not agency_code:
                    continue

                agency = self._get_or_create_agency(session, agency_name, {"agency_code": agency_code})
                if not agency:
                    continue

                # Create a payment record (aggregate level)
                source_id = f"{fiscal_year}-{agency_code}-{record.get('major_spending_category', '')}"

                # Check for duplicate
                existing = session.query(Payment).filter(
                    Payment.source_system == "socrata_expenditures",
                    Payment.source_id == source_id,
                ).first()
                if existing:
                    continue

                payment = Payment(
                    agency_id=agency.id,
                    amount=amount,
                    fiscal_year_state=fiscal_year,
                    description=record.get("major_spending_category", ""),
                    source_system="socrata_expenditures",
                    source_id=source_id,
                    raw_data=record,
                )
                session.add(payment)
                count += 1

        return count

    def _process_payment_batch(self, records: list[dict]) -> int:
        """Process a batch of payment records."""
        count = 0

        with get_session() as session:
            for record in records:
                payment = self._create_payment(session, record)
                if payment:
                    count += 1

        return count

    def _create_payment(self, session, record: dict) -> Optional[Payment]:
        """Create a payment record from Socrata data."""
        # Extract and validate amount
        amount_str = record.get("amount", record.get("payment_amount", "0"))
        try:
            amount = Decimal(str(amount_str).replace(",", "").replace("$", ""))
        except:
            return None

        if amount == 0:
            return None

        # Parse date
        date_str = record.get("date", record.get("payment_date", ""))
        payment_date = None
        if date_str:
            try:
                # Handle various date formats
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"]:
                    try:
                        payment_date = datetime.strptime(date_str[:19], fmt).date()
                        break
                    except ValueError:
                        continue
            except:
                pass

        # Get or create vendor
        vendor_name = record.get("payee_name", record.get("vendor_name", ""))
        vendor_id = None
        if vendor_name:
            vendor = self._get_or_create_vendor(session, vendor_name, record)
            vendor_id = vendor.id if vendor else None

        # Get or create agency
        agency_name = record.get("agency_name", record.get("agency", ""))
        agency_id = None
        if agency_name:
            agency = self._get_or_create_agency(session, agency_name, record)
            agency_id = agency.id if agency else None

        # Check for duplicate
        source_id = record.get(":id", record.get("id", ""))
        if source_id:
            existing = session.query(Payment).filter(
                Payment.source_system == "socrata",
                Payment.source_id == str(source_id),
            ).first()
            if existing:
                return None  # Skip duplicate

        # Calculate fiscal years
        fy = None
        if payment_date:
            fy = normalize_fiscal_years(payment_date)

        # Create payment record
        payment = Payment(
            vendor_id=vendor_id,
            agency_id=agency_id,
            amount=amount,
            payment_date=payment_date,
            fiscal_year_state=fy.state if fy else None,
            fiscal_year_federal=fy.federal if fy else None,
            calendar_year=fy.calendar if fy else None,
            comptroller_object_code=record.get("object_code", record.get("comptroller_object", "")),
            description=record.get("description", record.get("payment_description", "")),
            is_confidential=record.get("confidential", "N").upper() == "Y",
            source_system="socrata",
            source_id=str(source_id) if source_id else None,
            raw_data=record,
        )
        session.add(payment)

        return payment

    def _get_or_create_vendor(self, session, name: str, record: dict) -> Optional[Vendor]:
        """Get existing vendor or create new one."""
        if not name:
            return None

        normalized = normalize_vendor_name(name)

        # Try to find by normalized name
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == normalized
        ).first()

        if not vendor:
            # Create new vendor (will be enriched later with CMBL data)
            vendor = Vendor(
                name=name,
                name_normalized=normalized,
                in_cmbl=False,  # Not confirmed in CMBL yet
                first_seen=date.today(),
                last_seen=date.today(),
            )
            session.add(vendor)
            session.flush()

        return vendor

    def _get_or_create_agency(self, session, name: str, record: dict) -> Optional[Agency]:
        """Get existing agency or create new one."""
        if not name:
            return None

        # Try to extract agency code
        agency_code = record.get("agency_code", record.get("agency_number", ""))
        if not agency_code:
            # Generate code from name
            agency_code = "".join(word[0] for word in name.split()[:4]).upper()

        # Find by code or name
        agency = session.query(Agency).filter(
            (Agency.agency_code == agency_code) | (Agency.name == name)
        ).first()

        if not agency:
            agency = Agency(
                agency_code=agency_code,
                name=name,
            )
            session.add(agency)
            session.flush()

        return agency


# Additional Socrata query functions

def query_dataset(
    dataset_id: str,
    select: str = "*",
    where: str = None,
    limit: int = 1000,
    order: str = None,
) -> list[dict]:
    """
    Run a SoQL query against a Socrata dataset.

    Args:
        dataset_id: The 4x4 dataset identifier
        select: Fields to return
        where: SoQL where clause
        limit: Maximum records to return
        order: Order by clause

    Returns:
        List of result dicts
    """
    client = Socrata(
        SocrataIngestor.DOMAIN,
        app_token=config.socrata_token,
        timeout=60,
    )

    return client.get(
        dataset_id,
        select=select,
        where=where,
        limit=limit,
        order=order,
    )


def search_franchise_permits(business_name: str) -> list[dict]:
    """Search active franchise tax permits by business name."""
    return query_dataset(
        SocrataIngestor.DATASETS["franchise_tax_permits"],
        where=f"upper(taxpayer_name) like '%{business_name.upper()}%'",
        limit=100,
    )


def search_sales_tax_permits(business_name: str) -> list[dict]:
    """Search active sales tax permits by business name."""
    return query_dataset(
        SocrataIngestor.DATASETS["sales_tax_permits"],
        where=f"upper(taxpayer_name) like '%{business_name.upper()}%'",
        limit=100,
    )
