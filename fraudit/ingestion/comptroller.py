"""
Texas Comptroller detailed payments ingestor.

Fetches granular payment data from the Comptroller's transparency datasets,
which provides more detail than the aggregate Socrata expenditure data.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from sodapy import Socrata
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, Payment, Vendor, Agency
from fraudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class ComptrollerPaymentsIngestor(BaseIngestor):
    """
    Ingestor for Texas Comptroller detailed payment data.

    The Comptroller publishes more granular payment data than the aggregate
    expenditure datasets. This includes individual payments to vendors with
    specific dates, amounts, and object codes.
    """

    source_name = "comptroller_payments"

    # Socrata domain
    DOMAIN = "data.texas.gov"

    # Payments to Payee datasets (by fiscal year)
    # These provide individual payment records rather than aggregates
    PAYMENT_DATASETS = {
        # Vendor payments by fiscal year
        "payments_2024": "kfby-uvfk",  # FY2024 Vendor Payments
        "payments_2023": "4nks-wqxe",  # FY2023 Vendor Payments
        "payments_2022": "i78g-bx7k",  # FY2022 Vendor Payments
        "payments_2021": "727q-m4jx",  # FY2021 Vendor Payments
        "payments_2020": "gzyy-h98c",  # FY2020 Vendor Payments
    }

    # Alternative: Travel payments (more detailed)
    TRAVEL_DATASETS = {
        "travel_2024": "mxxv-rigo",
        "travel_2023": "26yq-rfie",
    }

    PAGE_SIZE = 50000

    def __init__(self):
        super().__init__()
        self.client = None
        self._agency_cache = {}
        self._vendor_cache = {}

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
        """Sync payment data from Comptroller."""
        print("Syncing detailed payment data from Texas Comptroller...")
        print("  Note: Detailed payment-to-payee datasets are not currently available on data.texas.gov")
        print("  The socrata_payments source provides aggregate expenditure data instead.")

        # The detailed vendor payment datasets (payments_2024, etc.) have been removed
        # from data.texas.gov. Only aggregate expenditure data is available.
        # Skip this sync - data is covered by socrata_payments ingestor.

        total_records = 0
        datasets_checked = 0

        # Try each dataset - most will 404 since they've been removed
        for dataset_name, dataset_id in self.PAYMENT_DATASETS.items():
            datasets_checked += 1
            try:
                print(f"  Checking {dataset_name}...")
                records = self._sync_payment_dataset(dataset_id, dataset_name)
                total_records += records
                if records > 0:
                    print(f"    {dataset_name}: {records:,} records")
            except Exception as e:
                if "404" in str(e):
                    print(f"    {dataset_name}: Dataset not available (removed from portal)")
                else:
                    print(f"    {dataset_name}: Error - {str(e)[:50]}")

        if total_records == 0:
            print("  No detailed payment datasets currently available.")
            print("  Use socrata_payments for state expenditure data.")

        return total_records

    def _sync_payment_dataset(self, dataset_id: str, dataset_name: str) -> int:
        """Sync a single payment dataset."""
        client = self._get_client()
        records_processed = 0

        # Get total count
        try:
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"]) if count_result else 0
        except Exception:
            total_count = 0

        if total_count == 0:
            return 0

        with get_session() as session:
            offset = 0

            with tqdm(total=total_count, desc=f"  {dataset_name}") as pbar:
                while True:
                    try:
                        results = client.get(
                            dataset_id,
                            limit=self.PAGE_SIZE,
                            offset=offset,
                            order=":id",
                        )
                    except Exception as e:
                        print(f"      API error at offset {offset}: {e}")
                        break

                    if not results:
                        break

                    for row in results:
                        try:
                            self._process_payment_row(session, row, dataset_name)
                            records_processed += 1
                        except Exception as e:
                            # Skip individual record errors
                            pass

                    pbar.update(len(results))
                    offset += len(results)

                    if len(results) < self.PAGE_SIZE:
                        break

                    # Commit periodically
                    if records_processed % 10000 == 0:
                        session.commit()

            session.commit()

        return records_processed

    def _process_payment_row(self, session, row: dict, source: str) -> None:
        """Process a single payment row."""
        # Extract vendor info
        vendor_name = row.get("payee_name") or row.get("vendor_name") or row.get("payee")
        if not vendor_name:
            return

        # Get or create vendor
        vendor = self._get_or_create_vendor(session, vendor_name, row)

        # Get or create agency
        agency_name = row.get("agency_name") or row.get("agency")
        agency = self._get_or_create_agency(session, agency_name, row) if agency_name else None

        # Parse amount
        amount_str = row.get("amount") or row.get("payment_amount") or row.get("expenditure_amount")
        if not amount_str:
            return

        try:
            amount = Decimal(str(amount_str).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            return

        # Parse date
        date_str = row.get("payment_date") or row.get("date") or row.get("check_date")
        payment_date = None
        if date_str:
            try:
                if "T" in str(date_str):
                    payment_date = datetime.fromisoformat(date_str.replace("Z", "")).date()
                else:
                    payment_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        # Parse fiscal year from dataset name or row
        fiscal_year = None
        fy_str = row.get("fiscal_year") or row.get("fy")
        if fy_str:
            try:
                fiscal_year = int(str(fy_str)[:4])
            except (ValueError, TypeError):
                pass

        if not fiscal_year and "202" in source:
            # Extract from dataset name like "payments_2024"
            for year in range(2020, 2030):
                if str(year) in source:
                    fiscal_year = year
                    break

        # Create payment record
        # Check for duplicate by amount, date, vendor, agency
        existing = session.query(Payment).filter(
            Payment.vendor_id == vendor.id if vendor else None,
            Payment.agency_id == agency.id if agency else None,
            Payment.amount == amount,
            Payment.payment_date == payment_date,
        ).first()

        if existing:
            return

        payment = Payment(
            vendor_id=vendor.id if vendor else None,
            agency_id=agency.id if agency else None,
            amount=amount,
            payment_date=payment_date,
            fiscal_year_state=fiscal_year,
            description=row.get("description") or row.get("comptroller_object_title"),
            comptroller_object_code=row.get("comptroller_object_code") or row.get("object_code"),
            source_system=f"comptroller_{source}",
            raw_data=row,
        )
        session.add(payment)

    def _sync_travel_dataset(self, dataset_id: str, dataset_name: str) -> int:
        """Sync travel payment dataset (more detailed payee info)."""
        # Use same logic as payment dataset
        return self._sync_payment_dataset(dataset_id, dataset_name)

    def _get_or_create_vendor(self, session, name: str, row: dict) -> Vendor:
        """Get or create a vendor record."""
        name_normalized = normalize_vendor_name(name)

        # Check cache first
        if name_normalized in self._vendor_cache:
            return self._vendor_cache[name_normalized]

        # Check database
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == name_normalized
        ).first()

        if not vendor:
            vendor = Vendor(
                name=name,
                name_normalized=name_normalized,
                city=row.get("payee_city") or row.get("city"),
                state=row.get("payee_state") or row.get("state"),
                zip_code=row.get("payee_zip") or row.get("zip"),
                raw_data={"source": "comptroller_payments"},
            )
            session.add(vendor)
            session.flush()

        self._vendor_cache[name_normalized] = vendor
        return vendor

    def _get_or_create_agency(self, session, name: str, row: dict) -> Agency:
        """Get or create an agency record."""
        # Check cache
        if name in self._agency_cache:
            return self._agency_cache[name]

        # Check database
        agency = session.query(Agency).filter(
            Agency.name == name
        ).first()

        if not agency:
            agency_code = row.get("agency_code") or row.get("agency_number") or name[:20]
            agency = Agency(
                agency_code=str(agency_code)[:20],
                name=name,
            )
            session.add(agency)
            session.flush()

        self._agency_cache[name] = agency
        return agency
