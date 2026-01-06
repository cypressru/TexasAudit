"""
TxSmartBuy / DIR Contract Data Ingestor.

Pulls contract data from Socrata datasets for DIR (Department of Information
Resources) contracts. These include IT contracts, software, services, etc.

Also attempts to pull from the Comptroller's contract search where available.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from sodapy import Socrata
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, Contract, Vendor, Agency
from fraudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class TxSmartBuyIngestor(BaseIngestor):
    """Ingestor for TxSmartBuy, DIR, and state contract data."""

    source_name = "txsmartbuy"

    # Socrata domain and dataset IDs
    DOMAIN = "data.texas.gov"

    # DIR Contract datasets on Socrata
    DATASETS = {
        # Active cooperative contracts
        "dir_active_contracts": "vipt-h4ye",
        # Contract sales data (actual purchases)
        "dir_contract_sales": "w64c-ndf7",
    }

    PAGE_SIZE = 10000

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
        """Sync contract data from Socrata datasets."""
        print("Syncing TxSmartBuy/DIR contract data...")
        print("  Downloading 2 datasets in parallel...")

        total = 0
        tasks = [
            ("DIR Active Contracts", self._sync_active_contracts),
            ("DIR Contract Sales", self._sync_contract_sales),
        ]

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_name = {
                executor.submit(func): name
                for name, func in tasks
            }

            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    count = future.result()
                    total += count
                    print(f"  ✓ {name}: {count:,} records")
                except Exception as e:
                    print(f"  ✗ {name}: {e}")

        return total

    def _sync_active_contracts(self) -> int:
        """Sync active DIR contracts from Socrata."""
        print("  Fetching DIR active contracts...")
        client = self._get_client()
        dataset_id = self.DATASETS["dir_active_contracts"]
        count = 0

        try:
            # Get total count
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"])
            print(f"  DIR Active Contracts: {total_count:,} records")

            if total_count == 0:
                return 0

            # Paginate through results
            offset = 0
            with tqdm(total=total_count, desc="DIR Contracts", leave=False) as pbar:
                while offset < total_count:
                    results = client.get(
                        dataset_id,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                    )

                    if not results:
                        break

                    processed = self._process_active_contracts_batch(results)
                    count += processed

                    offset += len(results)
                    pbar.update(len(results))

        except Exception as e:
            print(f"  DIR contracts error: {e}")

        return count

    def _process_active_contracts_batch(self, records: list[dict]) -> int:
        """Process a batch of active contract records."""
        count = 0
        seen_in_batch = set()

        with get_session() as session:
            for record in records:
                contract_number = record.get("contract_number", "")
                if not contract_number:
                    continue

                # Skip duplicates within this batch
                if contract_number in seen_in_batch:
                    continue
                seen_in_batch.add(contract_number)

                # Check for existing
                existing = session.query(Contract).filter(
                    Contract.contract_number == contract_number
                ).first()
                if existing:
                    continue

                # Parse dates
                start_date = None
                end_date = None
                if record.get("contract_start"):
                    try:
                        start_date = datetime.fromisoformat(
                            record["contract_start"][:10]
                        ).date()
                    except:
                        pass
                if record.get("contract_termination_date"):
                    try:
                        end_date = datetime.fromisoformat(
                            record["contract_termination_date"][:10]
                        ).date()
                    except:
                        pass

                # Get or create vendor
                vendor_name = record.get("primary_vendor_name", "")
                vendor_id = None
                if vendor_name:
                    vendor = self._get_or_create_vendor(session, vendor_name, record)
                    vendor_id = vendor.id if vendor else None

                # Description
                description = record.get("rfo_description", "")
                contract_type = record.get("contract_type", "")
                subtype = record.get("contract_subtype", "")
                if contract_type or subtype:
                    if description:
                        description = f"{description} [{contract_type}/{subtype}]"
                    else:
                        description = f"{contract_type}/{subtype}"

                contract = Contract(
                    contract_number=contract_number,
                    vendor_id=vendor_id,
                    description=description[:500] if description else None,
                    start_date=start_date,
                    end_date=end_date,
                    source="dir_contracts",
                    raw_data=record,
                )
                session.add(contract)
                count += 1

        return count

    def _sync_contract_sales(self) -> int:
        """Sync contract sales data (actual purchases under contracts)."""
        print("  Fetching DIR contract sales...")
        client = self._get_client()
        dataset_id = self.DATASETS["dir_contract_sales"]
        count = 0

        try:
            # Get total count
            count_result = client.get(dataset_id, select="count(*)")
            total_count = int(count_result[0]["count"])
            print(f"  DIR Contract Sales: {total_count:,} records")

            if total_count == 0:
                return 0

            # This dataset is large - limit to recent years
            # Get unique contracts and aggregate values
            offset = 0
            contracts_seen = set()

            with tqdm(total=min(total_count, 100000), desc="DIR Sales", leave=False) as pbar:
                while offset < min(total_count, 100000):
                    results = client.get(
                        dataset_id,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                        order="fiscal_year DESC",
                    )

                    if not results:
                        break

                    processed = self._process_sales_batch(results, contracts_seen)
                    count += processed

                    offset += len(results)
                    pbar.update(len(results))

        except Exception as e:
            print(f"  Contract sales error: {e}")

        return count

    def _process_sales_batch(self, records: list[dict], contracts_seen: set) -> int:
        """Process contract sales records - creates/updates contracts with values."""
        count = 0

        with get_session() as session:
            for record in records:
                contract_number = record.get("contract_number", "")
                if not contract_number:
                    continue

                # Parse purchase amount
                try:
                    amount = Decimal(str(record.get("purchase_amount", "0")))
                except:
                    continue

                if amount <= 0:
                    continue

                # Check if we already processed this contract
                if contract_number in contracts_seen:
                    # Update existing contract with additional value
                    existing = session.query(Contract).filter(
                        Contract.contract_number == contract_number
                    ).first()
                    if existing and existing.current_value:
                        existing.current_value += amount
                    continue

                contracts_seen.add(contract_number)

                # Check for existing in DB
                existing = session.query(Contract).filter(
                    Contract.contract_number == contract_number
                ).first()

                if existing:
                    # Update value
                    if existing.current_value:
                        existing.current_value += amount
                    else:
                        existing.current_value = amount
                    continue

                # Get or create vendor
                vendor_name = record.get("vendor_name", "")
                vendor_id = None
                if vendor_name:
                    vendor = self._get_or_create_vendor(session, vendor_name, record)
                    vendor_id = vendor.id if vendor else None

                # Get or create agency/customer
                customer_name = record.get("customer_name", "")
                agency_id = None
                if customer_name:
                    agency = self._get_or_create_agency(session, customer_name, record)
                    agency_id = agency.id if agency else None

                # Parse dates
                start_date = None
                end_date = None
                if record.get("contract_start_date"):
                    try:
                        start_date = datetime.fromisoformat(
                            record["contract_start_date"][:10]
                        ).date()
                    except:
                        pass
                if record.get("contract_termination_date"):
                    try:
                        end_date = datetime.fromisoformat(
                            record["contract_termination_date"][:10]
                        ).date()
                    except:
                        pass

                description = record.get("rfo_description", "")

                contract = Contract(
                    contract_number=contract_number,
                    vendor_id=vendor_id,
                    agency_id=agency_id,
                    description=description[:500] if description else None,
                    current_value=amount,
                    start_date=start_date,
                    end_date=end_date,
                    source="dir_sales",
                    raw_data=record,
                )
                session.add(contract)
                count += 1

        return count

    def _get_or_create_vendor(self, session, name: str, record: dict) -> Optional[Vendor]:
        """Get or create vendor."""
        if not name:
            return None

        normalized = normalize_vendor_name(name)
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == normalized
        ).first()

        if not vendor:
            # Extract HUB status if available
            hub_status = record.get("primary_vendor_hub_status", "") or record.get("vendor_hub_type", "")
            is_hub = hub_status and hub_status not in ("N/A", "Non HUB", "")

            vendor = Vendor(
                name=name,
                name_normalized=normalized,
                hub_status=hub_status[:50] if hub_status else None,
                in_cmbl=False,
                first_seen=date.today(),
                last_seen=date.today(),
            )
            session.add(vendor)
            session.flush()

        return vendor

    def _get_or_create_agency(self, session, name: str, record: dict) -> Optional[Agency]:
        """Get or create agency."""
        if not name:
            return None

        # Try to find by name first
        agency = session.query(Agency).filter(Agency.name == name).first()

        if not agency:
            # Generate a code
            code = "".join(word[0] for word in name.split()[:4]).upper()

            # Check if this code already exists
            existing_code = session.query(Agency).filter(Agency.agency_code == code).first()
            if existing_code:
                # Use the existing agency if code matches, even if name differs
                # Or append a number to make unique
                base_code = code
                counter = 2
                while session.query(Agency).filter(Agency.agency_code == code).first():
                    code = f"{base_code}{counter}"
                    counter += 1

            agency = Agency(
                agency_code=code,
                name=name,
            )
            session.add(agency)
            session.flush()

        return agency
