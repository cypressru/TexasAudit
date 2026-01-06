"""
TxDOT (Texas Department of Transportation) data ingestor.

Fetches bid tabulations and contract data from data.texas.gov.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from sodapy import Socrata
from tqdm import tqdm

from texasaudit.config import config
from texasaudit.database import get_session, ConstructionBid, Contract, Vendor, Agency
from texasaudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class TxDOTBidIngestor(BaseIngestor):
    """Ingestor for TxDOT bid tabulation data."""

    source_name = "txdot_bids"

    DOMAIN = "data.texas.gov"
    DATASET_ID = "de7b-7dna"  # Bid Tabulations
    PAGE_SIZE = 10000

    def __init__(self):
        super().__init__()
        self.client = None

    def _get_client(self) -> Socrata:
        """Get or create Socrata client."""
        if self.client is None:
            self.client = Socrata(
                self.DOMAIN,
                app_token=config.socrata_token,
                timeout=120,
            )
        return self.client

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """Sync TxDOT bid tabulation data."""
        print("Syncing TxDOT bid tabulations from data.texas.gov...")

        client = self._get_client()

        # Get total count
        try:
            count_result = client.get(self.DATASET_ID, select="count(*)")
            total_count = int(count_result[0]["count"])
            print(f"  Total records available: {total_count:,}")
        except Exception as e:
            print(f"  Error getting count: {e}")
            return 0

        # Limit to recent data (last 2 years of lettings)
        two_years_ago = (datetime.now().year - 2)
        where_clause = f"project_actual_let_date >= '{two_years_ago}-01-01'"

        try:
            count_result = client.get(
                self.DATASET_ID,
                select="count(*)",
                where=where_clause
            )
            filtered_count = int(count_result[0]["count"])
            print(f"  Records since {two_years_ago}: {filtered_count:,}")
        except Exception as e:
            print(f"  Using full dataset: {e}")
            where_clause = None
            filtered_count = total_count

        # Paginate through results
        offset = 0
        records_processed = 0

        with tqdm(total=filtered_count, desc="TxDOT Bids") as pbar:
            while offset < filtered_count:
                try:
                    results = client.get(
                        self.DATASET_ID,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                        where=where_clause,
                        order="project_actual_let_date DESC",
                    )

                    if not results:
                        break

                    # Process batch
                    processed = self._process_bid_batch(results)
                    records_processed += processed

                    offset += len(results)
                    pbar.update(len(results))

                except Exception as e:
                    print(f"\n  Error at offset {offset}: {e}")
                    offset += self.PAGE_SIZE  # Skip to next batch

        return records_processed

    def _process_bid_batch(self, records: list[dict]) -> int:
        """Process a batch of bid records."""
        count = 0
        seen_bids = set()  # Track bids within this batch

        with get_session() as session:
            for record in records:
                bid = self._create_bid(session, record, seen_bids)
                if bid:
                    count += 1

        return count

    def _create_bid(self, session, record: dict, seen_bids: set = None) -> Optional[ConstructionBid]:
        """Create a bid record from TxDOT data."""
        if seen_bids is None:
            seen_bids = set()

        # Extract project ID (CSJ)
        project_id = record.get("control_section_job_csj", "")
        if not project_id:
            project_id = record.get("controlling_project_id_ccsj", "")
        if not project_id:
            return None

        # Extract contractor name (field is vendor_name in this dataset)
        contractor_name = record.get("vendor_name", "")
        if not contractor_name:
            return None

        # Skip engineer's estimates (not actual bids)
        if "engineer" in contractor_name.lower() and "estimate" in contractor_name.lower():
            return None

        # Check for duplicate within current batch
        contractor_normalized = normalize_vendor_name(contractor_name)
        bid_key = f"{project_id}|{contractor_normalized}"
        if bid_key in seen_bids:
            return None
        seen_bids.add(bid_key)

        # Check for duplicate in database
        existing = session.query(ConstructionBid).filter(
            ConstructionBid.project_id == project_id,
            ConstructionBid.contractor_normalized == contractor_normalized,
        ).first()
        if existing:
            return None

        # Parse bid amount
        bid_amount = None
        bid_str = record.get("bid_total_amount", "")
        if bid_str:
            try:
                bid_amount = Decimal(str(bid_str).replace(",", "").replace("$", ""))
            except:
                pass

        # Parse engineer estimate
        engineer_estimate = None
        est_str = record.get("sealed_engineer_s_estimate", "") or record.get("sealed_engineer_s_estimate_1", "")
        if est_str:
            try:
                engineer_estimate = Decimal(str(est_str).replace(",", "").replace("$", ""))
            except:
                pass

        # Parse bid rank (can be "EE" for engineer estimate, so handle non-numeric)
        bid_rank = None
        rank_str = record.get("bid_rank_sequence_number", "")
        if rank_str:
            try:
                bid_rank = int(float(rank_str))
            except:
                pass

        # Parse letting date
        letting_date = None
        date_str = record.get("project_actual_let_date", "") or record.get("project_estimated_let_date", "")
        if date_str:
            try:
                letting_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except:
                pass

        # Try to link to existing vendor
        vendor_id = None
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == contractor_normalized
        ).first()
        if vendor:
            vendor_id = vendor.id

        # Create bid record
        bid = ConstructionBid(
            project_id=project_id,
            contractor_name=contractor_name,
            contractor_normalized=contractor_normalized,
            vendor_id=vendor_id,
            bid_amount=bid_amount,
            engineer_estimate=engineer_estimate,
            bid_rank=bid_rank,
            is_winner=(bid_rank == 1),
            letting_date=letting_date,
            county=record.get("county", ""),
            district=record.get("district_division", ""),
            project_description=record.get("short_description", "") or record.get("project_name", ""),
            work_type=record.get("project_type", ""),
            source="txdot",
            raw_data=record,
        )
        session.add(bid)
        return bid


class TxDOTContractIngestor(BaseIngestor):
    """Ingestor for TxDOT completed contract data (Recapitulation)."""

    source_name = "txdot_contracts"

    DOMAIN = "data.texas.gov"
    DATASET_ID = "h3h6-qwdh"  # Recapitulation
    PAGE_SIZE = 5000

    def __init__(self):
        super().__init__()
        self.client = None

    def _get_client(self) -> Socrata:
        """Get or create Socrata client."""
        if self.client is None:
            self.client = Socrata(
                self.DOMAIN,
                app_token=config.socrata_token,
                timeout=120,
            )
        return self.client

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """Sync TxDOT contract/recapitulation data."""
        print("Syncing TxDOT contracts from data.texas.gov...")

        client = self._get_client()

        # Get total count
        try:
            count_result = client.get(self.DATASET_ID, select="count(*)")
            total_count = int(count_result[0]["count"])
            print(f"  Total records available: {total_count:,}")
        except Exception as e:
            print(f"  Error getting count: {e}")
            return 0

        # Paginate through results
        offset = 0
        records_processed = 0

        with tqdm(total=total_count, desc="TxDOT Contracts") as pbar:
            while offset < total_count:
                try:
                    results = client.get(
                        self.DATASET_ID,
                        limit=self.PAGE_SIZE,
                        offset=offset,
                    )

                    if not results:
                        break

                    # Process batch
                    processed = self._process_contract_batch(results)
                    records_processed += processed

                    offset += len(results)
                    pbar.update(len(results))

                except Exception as e:
                    print(f"\n  Error at offset {offset}: {e}")
                    offset += self.PAGE_SIZE

        return records_processed

    def _process_contract_batch(self, records: list[dict]) -> int:
        """Process a batch of contract records."""
        count = 0
        seen_contracts = set()  # Track contracts within this batch

        with get_session() as session:
            # Get or create TxDOT agency
            txdot_agency = self._get_txdot_agency(session)

            for record in records:
                contract = self._create_contract(session, record, txdot_agency, seen_contracts)
                if contract:
                    count += 1

        return count

    def _get_txdot_agency(self, session) -> Agency:
        """Get or create TxDOT agency record."""
        agency = session.query(Agency).filter(
            Agency.agency_code == "TXDOT"
        ).first()

        if not agency:
            agency = Agency(
                agency_code="TXDOT",
                name="TEXAS DEPARTMENT OF TRANSPORTATION",
            )
            session.add(agency)
            session.flush()

        return agency

    def _create_contract(self, session, record: dict, agency: Agency, seen_contracts: set) -> Optional[Contract]:
        """Create a contract record from TxDOT recapitulation data."""
        # Extract contract number
        contract_number = record.get("contract_number", "")
        if not contract_number:
            return None

        # Get CSJ for uniqueness (multiple rows per contract in source data)
        ccsj = record.get("controlling_project_id_ccsj", "")

        # Make contract number unique to TxDOT with CSJ
        unique_contract_num = f"TXDOT-{contract_number}-{ccsj}" if ccsj else f"TXDOT-{contract_number}"

        # Check for duplicate in current batch
        if unique_contract_num in seen_contracts:
            return None
        seen_contracts.add(unique_contract_num)

        # Check for duplicate in database
        existing = session.query(Contract).filter(
            Contract.contract_number == unique_contract_num
        ).first()
        if existing:
            return None

        # Get contractor name and link to vendor
        contractor_name = record.get("contractor", "")
        vendor_id = None
        if contractor_name:
            contractor_normalized = normalize_vendor_name(contractor_name)
            vendor = session.query(Vendor).filter(
                Vendor.name_normalized == contractor_normalized
            ).first()

            if not vendor:
                # Create new vendor
                vendor = Vendor(
                    name=contractor_name,
                    name_normalized=contractor_normalized,
                    in_cmbl=False,
                    first_seen=date.today(),
                    last_seen=date.today(),
                )
                session.add(vendor)
                session.flush()

            vendor_id = vendor.id

        # Parse dates
        start_date = None
        date_str = record.get("date_work_begin", "")
        if date_str:
            try:
                start_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except:
                pass

        end_date = None
        end_str = record.get("date_work_accepted", "")
        if end_str:
            try:
                end_date = datetime.strptime(end_str[:10], "%Y-%m-%d").date()
            except:
                pass

        # Parse amounts
        current_value = None
        amount_str = record.get("final_contract_amount", record.get("original_contract_amount", ""))
        if amount_str:
            try:
                current_value = Decimal(str(amount_str).replace(",", "").replace("$", ""))
            except:
                pass

        max_value = None
        max_str = record.get("original_contract_amount", "")
        if max_str:
            try:
                max_value = Decimal(str(max_str).replace(",", "").replace("$", ""))
            except:
                pass

        # Create contract
        contract = Contract(
            contract_number=unique_contract_num,
            vendor_id=vendor_id,
            agency_id=agency.id,
            description=f"{record.get('county', '')} - {record.get('contract_limits_from', '')} to {record.get('contract_limits_to', '')}",
            current_value=current_value,
            max_value=max_value,
            start_date=start_date,
            end_date=end_date,
            source="txdot",
            raw_data=record,
        )
        session.add(contract)
        return contract
