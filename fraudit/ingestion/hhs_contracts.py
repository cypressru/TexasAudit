"""
HHS (Health and Human Services) contracts scraper.

Scrapes contract data from contracts.hhs.texas.gov.
"""

import re
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from fraudit.database import get_session, HHSContract, Vendor
from fraudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class HHSContractsIngestor(BaseIngestor):
    """Ingestor for HHS contract data via web scraping."""

    source_name = "hhs_contracts"

    BASE_URL = "https://contracts.hhs.texas.gov"
    LIST_URL = f"{BASE_URL}/api/views/contracts"

    # Rate limiting
    REQUEST_DELAY = 0.5  # seconds between requests

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """Sync HHS contract data."""
        print("Syncing HHS contracts from contracts.hhs.texas.gov...")

        # Try API approach first
        contracts = self._fetch_contracts_api()

        if not contracts:
            print("  API not available, trying web scrape...")
            contracts = self._fetch_contracts_scrape()

        if not contracts:
            print("  No contracts fetched")
            return 0

        print(f"  Found {len(contracts):,} contracts")

        # Process contracts
        count = 0
        with get_session() as session:
            for contract_data in tqdm(contracts, desc="HHS Contracts"):
                contract = self._create_contract(session, contract_data)
                if contract:
                    count += 1

        return count

    def _fetch_contracts_api(self) -> list[dict]:
        """Try to fetch contracts via API/JSON endpoint."""
        contracts = []

        try:
            # The site uses DataTables - try to find the AJAX endpoint
            # Check if there's a JSON API
            response = self.session.get(
                self.BASE_URL,
                timeout=30,
            )

            if response.status_code != 200:
                return []

            # Parse the main page to find data table configuration
            soup = BeautifulSoup(response.text, "html.parser")

            # Look for DataTables AJAX configuration
            scripts = soup.find_all("script")
            for script in scripts:
                if script.string and "ajax" in script.string.lower():
                    # Found potential AJAX config
                    pass

            # Try common DataTables endpoints
            endpoints = [
                f"{self.BASE_URL}/api/contracts",
                f"{self.BASE_URL}/contracts/data",
                f"{self.BASE_URL}/api/v1/contracts",
            ]

            for endpoint in endpoints:
                try:
                    resp = self.session.get(endpoint, timeout=30)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list):
                            return data
                        elif isinstance(data, dict) and "data" in data:
                            return data["data"]
                except:
                    continue

            return []

        except Exception as e:
            print(f"  API fetch error: {e}")
            return []

    def _fetch_contracts_scrape(self) -> list[dict]:
        """Fetch contracts via web scraping."""
        contracts = []

        try:
            # Get main page
            response = self.session.get(self.BASE_URL, timeout=30)
            if response.status_code != 200:
                print(f"  Failed to fetch main page: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, "html.parser")

            # Find the contracts table
            table = soup.find("table", {"id": "contracts-table"})
            if not table:
                table = soup.find("table", class_=lambda x: x and "dataTable" in x)
            if not table:
                table = soup.find("table")

            if not table:
                print("  Could not find contracts table")
                return []

            # Get headers
            headers = []
            header_row = table.find("thead")
            if header_row:
                headers = [th.get_text(strip=True).lower().replace(" ", "_")
                          for th in header_row.find_all("th")]

            # Get rows
            tbody = table.find("tbody")
            if tbody:
                rows = tbody.find_all("tr")
            else:
                rows = table.find_all("tr")[1:]  # Skip header row

            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 3:  # At least contract#, vendor, date
                    contract_data = {}

                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            key = headers[i]
                        else:
                            key = f"col_{i}"

                        # Get text and any links
                        text = cell.get_text(strip=True)
                        link = cell.find("a")

                        contract_data[key] = text
                        if link and link.get("href"):
                            contract_data[f"{key}_link"] = link.get("href")

                    if contract_data:
                        contracts.append(contract_data)

            # Paginate if needed
            # Look for pagination links
            page = 2
            max_pages = 200  # Safety limit

            while page <= max_pages:
                time.sleep(self.REQUEST_DELAY)

                # Try common pagination patterns
                next_url = None
                pagination = soup.find("ul", class_="pagination")
                if pagination:
                    next_link = pagination.find("a", string=re.compile(r"next|>|»", re.I))
                    if next_link:
                        next_url = next_link.get("href")

                if not next_url:
                    # Try query parameter
                    next_url = f"{self.BASE_URL}?page={page}"

                try:
                    resp = self.session.get(next_url, timeout=30)
                    if resp.status_code != 200:
                        break

                    soup = BeautifulSoup(resp.text, "html.parser")
                    table = soup.find("table")
                    if not table:
                        break

                    tbody = table.find("tbody")
                    if tbody:
                        rows = tbody.find_all("tr")
                    else:
                        rows = table.find_all("tr")[1:]

                    if not rows:
                        break

                    new_contracts = 0
                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        if len(cells) >= 3:
                            contract_data = {}
                            for i, cell in enumerate(cells):
                                if i < len(headers):
                                    key = headers[i]
                                else:
                                    key = f"col_{i}"
                                contract_data[key] = cell.get_text(strip=True)

                            if contract_data:
                                contracts.append(contract_data)
                                new_contracts += 1

                    if new_contracts == 0:
                        break

                    page += 1

                except Exception as e:
                    break

            return contracts

        except Exception as e:
            print(f"  Scrape error: {e}")
            return []

    def _create_contract(self, session, data: dict) -> Optional[HHSContract]:
        """Create an HHS contract record."""
        # Extract contract number
        contract_number = (
            data.get("contract_number") or
            data.get("contract") or
            data.get("contract_#") or
            data.get("col_0", "")
        )
        if not contract_number:
            return None

        # Clean contract number
        contract_number = contract_number.strip()

        # Check for duplicate
        existing = session.query(HHSContract).filter(
            HHSContract.contract_number == contract_number
        ).first()
        if existing:
            return None

        # Extract vendor name
        vendor_name = (
            data.get("vendor") or
            data.get("vendor_name") or
            data.get("contractor") or
            data.get("col_1", "")
        )
        if not vendor_name:
            return None

        vendor_normalized = normalize_vendor_name(vendor_name)

        # Try to link to existing vendor
        vendor_id = None
        vendor = session.query(Vendor).filter(
            Vendor.name_normalized == vendor_normalized
        ).first()
        if vendor:
            vendor_id = vendor.id

        # Parse start date
        start_date = None
        date_str = (
            data.get("start_date") or
            data.get("effective_date") or
            data.get("date") or
            data.get("col_2", "")
        )
        if date_str:
            start_date = self._parse_date(date_str)

        # Parse end date
        end_date = None
        end_str = data.get("end_date") or data.get("expiration_date", "")
        if end_str:
            end_date = self._parse_date(end_str)

        # Extract solicitation
        solicitation = (
            data.get("solicitation") or
            data.get("solicitation_number") or
            data.get("rfp") or
            data.get("col_3", "")
        )

        # Create contract
        contract = HHSContract(
            contract_number=contract_number,
            vendor_name=vendor_name,
            vendor_normalized=vendor_normalized,
            vendor_id=vendor_id,
            solicitation_number=solicitation,
            start_date=start_date,
            end_date=end_date,
            agency="hhsc",
            raw_data=data,
        )
        session.add(contract)
        return contract

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string in various formats."""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try various formats
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y/%m/%d",
            "%d-%b-%Y",
            "%b %d, %Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None
