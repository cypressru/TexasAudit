"""
USASpending.gov API client for federal funds flowing to Texas.

Texas received over $41.7 billion in federal HHS funds alone in FY 2024.
This data is essential for reconciling state-reported spending with federal sources.

API Documentation: https://api.usaspending.gov
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional

import requests
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, Grant, Vendor, Agency
from fraudit.normalization import normalize_vendor_name, normalize_fiscal_years
from .base import BaseIngestor


class USASpendingIngestor(BaseIngestor):
    """Ingestor for USASpending.gov federal spending data."""

    source_name = "usaspending"

    BASE_URL = "https://api.usaspending.gov/api/v2"

    # Texas FIPS code
    TEXAS_FIPS = "48"

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
        })

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Sync federal spending data for Texas.

        Pulls grants, contracts, and other federal awards to Texas recipients.
        """
        total = 0

        # Sync grants
        print("Syncing federal grants to Texas...")
        total += self._sync_awards("grants", since)

        # Sync contracts
        print("Syncing federal contracts to Texas...")
        total += self._sync_awards("contracts", since)

        return total

    def _sync_awards(self, award_type: str, since: Optional[datetime] = None) -> int:
        """Sync awards of a specific type."""
        # Determine fiscal years to query
        current_fy = normalize_fiscal_years(date.today()).federal
        start_fy = config.start_fiscal_year or 2015

        count = 0
        for fy in range(start_fy, current_fy + 1):
            print(f"  Fetching FY {fy} {award_type}...")
            fy_count = self._fetch_fiscal_year_awards(award_type, fy)
            count += fy_count

        return count

    def _fetch_fiscal_year_awards(self, award_type: str, fiscal_year: int) -> int:
        """Fetch all awards for a fiscal year."""
        # Map award type to API endpoint and type codes
        if award_type == "grants":
            type_codes = ["02", "03", "04", "05"]  # Grant types
        else:
            type_codes = ["A", "B", "C", "D"]  # Contract types

        # Build search filters
        filters = {
            "place_of_performance_locations": [
                {"country": "USA", "state": "TX"}
            ],
            "time_period": [
                {
                    "start_date": f"{fiscal_year - 1}-10-01",
                    "end_date": f"{fiscal_year}-09-30",
                }
            ],
            "award_type_codes": type_codes,
        }

        # Use spending_by_award endpoint for detailed data
        page = 1
        count = 0

        while True:
            response = self._api_request(
                "/search/spending_by_award/",
                {
                    "filters": filters,
                    "fields": [
                        "Award ID",
                        "Recipient Name",
                        "Award Amount",
                        "Total Outlays",
                        "Description",
                        "Start Date",
                        "End Date",
                        "Awarding Agency",
                        "Awarding Sub Agency",
                        "recipient_id",
                        "Place of Performance City",
                        "Place of Performance State Code",
                    ],
                    "page": page,
                    "limit": 100,
                    "sort": "Award Amount",
                    "order": "desc",
                },
            )

            if not response or "results" not in response:
                break

            results = response["results"]
            if not results:
                break

            # Process batch
            batch_count = self._process_award_batch(results, award_type, fiscal_year)
            count += batch_count

            # Check if more pages
            if len(results) < 100:
                break

            page += 1

            # Safety limit
            if page > 1000:
                break

        return count

    def _api_request(self, endpoint: str, payload: dict) -> Optional[dict]:
        """Make an API request to USASpending."""
        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = self.session.post(url, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"  API error: {e}")
            return None

    def _process_award_batch(self, results: list, award_type: str, fiscal_year: int) -> int:
        """Process a batch of award results."""
        count = 0

        with get_session() as session:
            for record in results:
                grant = self._create_grant(session, record, award_type, fiscal_year)
                if grant:
                    count += 1

        return count

    def _create_grant(self, session, record: dict, award_type: str, fiscal_year: int) -> Optional[Grant]:
        """Create a grant record from USASpending data."""
        award_id = record.get("Award ID", "")
        if not award_id:
            return None

        # Check for existing
        existing = session.query(Grant).filter(
            Grant.federal_award_id == award_id
        ).first()

        if existing:
            return None  # Skip duplicate

        # Get or create recipient as vendor
        recipient_name = record.get("Recipient Name", "")
        recipient = None
        if recipient_name:
            recipient = self._get_or_create_vendor(session, recipient_name)

        # Get or create awarding agency
        agency_name = record.get("Awarding Agency", "") or record.get("Awarding Sub Agency", "")
        agency = None
        if agency_name:
            agency = self._get_or_create_agency(session, agency_name)

        # Parse amounts
        amount_awarded = None
        amount_disbursed = None

        try:
            amount_str = record.get("Award Amount", "0")
            if amount_str:
                amount_awarded = Decimal(str(amount_str).replace(",", ""))
        except:
            pass

        try:
            outlay_str = record.get("Total Outlays", "0")
            if outlay_str:
                amount_disbursed = Decimal(str(outlay_str).replace(",", ""))
        except:
            pass

        # Parse dates
        start_date = None
        end_date = None

        try:
            start_str = record.get("Start Date", "")
            if start_str:
                start_date = datetime.strptime(start_str[:10], "%Y-%m-%d").date()
        except:
            pass

        try:
            end_str = record.get("End Date", "")
            if end_str:
                end_date = datetime.strptime(end_str[:10], "%Y-%m-%d").date()
        except:
            pass

        # Create grant record
        grant = Grant(
            grant_number=award_id,
            recipient_id=recipient.id if recipient else None,
            agency_id=agency.id if agency else None,
            program_name=record.get("Description", "")[:500] if record.get("Description") else None,
            amount_awarded=amount_awarded,
            amount_disbursed=amount_disbursed,
            fiscal_year=fiscal_year,
            start_date=start_date,
            end_date=end_date,
            source=f"usaspending_{award_type}",
            federal_award_id=award_id,
            raw_data=record,
        )
        session.add(grant)

        return grant

    def _get_or_create_vendor(self, session, name: str) -> Optional[Vendor]:
        """Get or create vendor."""
        if not name:
            return None

        normalized = normalize_vendor_name(name)
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == normalized
        ).first()

        if not vendor:
            vendor = Vendor(
                name=name,
                name_normalized=normalized,
                in_cmbl=False,
                first_seen=date.today(),
                last_seen=date.today(),
            )
            session.add(vendor)
            session.flush()

        return vendor

    def _get_or_create_agency(self, session, name: str) -> Optional[Agency]:
        """Get or create federal agency with unique code."""
        if not name:
            return None

        agency = session.query(Agency).filter(Agency.name == name).first()

        if not agency:
            # Generate base code from initials, prefixed with FED_ to avoid
            # conflicts with state agency codes (e.g., DOE = Dept of Energy)
            base_code = "".join(word[0] for word in name.split()[:4]).upper()
            code = f"FED_{base_code}"

            # Check if code exists and make unique if needed
            existing = session.query(Agency).filter(Agency.agency_code == code).first()
            if existing:
                # Append counter to make unique
                counter = 2
                while session.query(Agency).filter(Agency.agency_code == f"{code}_{counter}").first():
                    counter += 1
                code = f"{code}_{counter}"

            agency = Agency(agency_code=code, name=name, category="federal")
            session.add(agency)
            session.flush()

        return agency


# Additional USASpending query functions

def search_texas_recipients(
    recipient_name: str = None,
    award_type: str = "all",
    fiscal_year: int = None,
    limit: int = 100,
) -> list[dict]:
    """
    Search for federal awards to Texas recipients.

    Args:
        recipient_name: Filter by recipient name
        award_type: 'grants', 'contracts', or 'all'
        fiscal_year: Filter by fiscal year
        limit: Maximum results

    Returns:
        List of award records
    """
    ingestor = USASpendingIngestor()

    filters = {
        "place_of_performance_locations": [
            {"country": "USA", "state": "TX"}
        ],
    }

    if recipient_name:
        filters["recipient_search_text"] = recipient_name

    if award_type == "grants":
        filters["award_type_codes"] = ["02", "03", "04", "05"]
    elif award_type == "contracts":
        filters["award_type_codes"] = ["A", "B", "C", "D"]

    if fiscal_year:
        filters["time_period"] = [{
            "start_date": f"{fiscal_year - 1}-10-01",
            "end_date": f"{fiscal_year}-09-30",
        }]

    response = ingestor._api_request(
        "/search/spending_by_award/",
        {
            "filters": filters,
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Description", "Awarding Agency", "Start Date",
            ],
            "limit": limit,
            "sort": "Award Amount",
            "order": "desc",
        },
    )

    return response.get("results", []) if response else []


def get_agency_profile(agency_code: str) -> Optional[dict]:
    """Get federal agency profile and spending data."""
    ingestor = USASpendingIngestor()

    response = ingestor._api_request(
        f"/agency/{agency_code}/",
        {},
    )

    return response
