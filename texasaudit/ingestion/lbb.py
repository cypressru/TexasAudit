"""
LBB (Legislative Budget Board) Contracts Database ingestion.

The LBB maintains the most comprehensive contract database for Texas state agencies.
No API available - requires manual Excel/CSV export or scraping.

Source: https://contracts.lbb.texas.gov
"""

import csv
import io
import re
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

from texasaudit.config import config
from texasaudit.database import get_session, Contract, Vendor, Agency
from texasaudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class LBBIngestor(BaseIngestor):
    """
    Ingestor for LBB Contracts Database.

    Note: LBB doesn't provide a public API, so this ingestor supports:
    1. Manual Excel file import (from browser export)
    2. Limited search functionality via web scraping (if enabled)
    """

    source_name = "lbb_contracts"

    LBB_URL = "https://contracts.lbb.texas.gov"

    # Reporting thresholds
    THRESHOLDS = {
        "general": 50000,
        "consulting": 14000,
        "professional": 14000,
        "construction": 14000,
        "major_it": 100000,
    }

    def __init__(self):
        super().__init__()
        self.data_dir = config.data_dir
        self.import_dir = self.data_dir / "lbb_imports"
        self.import_dir.mkdir(parents=True, exist_ok=True)

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Import LBB contracts from Excel or CSV files.

        Since LBB doesn't have an API, this looks for export files in
        the import directory that the user has manually downloaded.
        """
        # Find files to import (Excel and CSV)
        import_files = (
            list(self.import_dir.glob("*.xlsx")) +
            list(self.import_dir.glob("*.xls")) +
            list(self.import_dir.glob("*.csv"))
        )

        if not import_files:
            print(f"  No import files found in {self.import_dir}")
            print(f"  To import LBB data:")
            print(f"  1. Go to {self.LBB_URL}")
            print(f"  2. Search for contracts and export to Excel/CSV")
            print(f"  3. Save the file to {self.import_dir}")
            return 0

        total = 0
        for import_file in import_files:
            print(f"  Importing {import_file.name}...")
            if import_file.suffix.lower() == ".csv":
                count = self._import_csv(import_file)
            else:
                count = self._import_excel(import_file)
            total += count
            print(f"  Imported {count:,} contracts from {import_file.name}")

            # Move processed file to archive
            archive_dir = self.import_dir / "processed"
            archive_dir.mkdir(exist_ok=True)
            import_file.rename(archive_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{import_file.name}")

        return total

    def _import_csv(self, file_path: Path) -> int:
        """Import contracts from an LBB CSV export."""
        count = 0
        seen_in_session = set()  # Track contracts we've added in this session

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        print(f"  Found {len(rows):,} rows in CSV")

        with get_session() as session:
            # Pre-load existing contract numbers to avoid repeated queries
            existing = set(
                c[0] for c in session.query(Contract.contract_number).all()
            )
            print(f"  {len(existing):,} contracts already in database")

            for row in tqdm(rows, desc="Importing LBB contracts"):
                contract = self._create_contract_from_csv(
                    session, row, existing, seen_in_session
                )
                if contract:
                    count += 1

                if count % 1000 == 0 and count > 0:
                    session.commit()

        return count

    def _create_contract_from_csv(
        self, session, row: dict, existing: set, seen_in_session: set
    ) -> Optional[Contract]:
        """Create a contract record from LBB CSV row."""
        # Extract contract number - prefer Contract-ID, fall back to Contract
        contract_number = row.get("Contract-ID", "") or row.get("Contract", "")

        # Clean HTML from contract number if present
        if "<a href=" in contract_number:
            # Extract just the contract ID from the link text
            match = re.search(r">([^<]+)</a>", contract_number)
            if match:
                contract_number = match.group(1)

        contract_number = contract_number.strip()
        if not contract_number:
            return None

        # Skip if already in database or seen in this session
        if contract_number in existing or contract_number in seen_in_session:
            return None

        seen_in_session.add(contract_number)

        # Vendor
        vendor_name = row.get("Vendor", "").strip()
        vendor_id = None
        if vendor_name:
            vendor = self._get_or_create_vendor(session, vendor_name)
            vendor_id = vendor.id if vendor else None

        # Agency
        agency_name = row.get("Agency", "").strip()
        agency_id = None
        if agency_name:
            agency = self._get_or_create_agency(session, agency_name)
            agency_id = agency.id if agency else None

        # Description/Subject
        description = row.get("Subject", "").strip()

        # Parse contract value - remove $ and commas
        current_value = None
        value_str = row.get("Current Contract Value", "").strip()
        if value_str:
            try:
                value_str = value_str.replace("$", "").replace(",", "")
                current_value = Decimal(value_str)
            except:
                pass

        # Parse dates
        start_date = None
        end_date = None

        award_date = row.get("Award Date", "").strip()
        if award_date:
            try:
                start_date = datetime.strptime(award_date, "%Y-%m-%d").date()
            except:
                try:
                    start_date = datetime.strptime(award_date, "%m/%d/%Y").date()
                except:
                    pass

        completion_date = row.get("Completion Date", "").strip()
        if completion_date:
            try:
                end_date = datetime.strptime(completion_date, "%m/%d/%Y").date()
            except:
                try:
                    end_date = datetime.strptime(completion_date, "%Y-%m-%d").date()
                except:
                    pass

        # NIGP codes
        nigp_str = row.get("NGIP Codes and Categories", "") or row.get("NIGP Codes and Categories", "")
        nigp_codes = None
        if nigp_str:
            # Extract numeric codes from strings like "948-65 Medical Services"
            codes = re.findall(r"(\d{3}-\d{2})", nigp_str)
            if codes:
                nigp_codes = codes

        # Category and procurement method for metadata
        category = row.get("Category", "").strip()
        procurement = row.get("Procurement Method", "").strip()
        status = row.get("Status", "").strip()

        contract = Contract(
            contract_number=contract_number,
            vendor_id=vendor_id,
            agency_id=agency_id,
            description=description[:500] if description else None,
            current_value=current_value,
            start_date=start_date,
            end_date=end_date,
            nigp_codes=nigp_codes,
            source="LBB",
            raw_data={
                **row,
                "category": category,
                "procurement_method": procurement,
                "status": status,
            },
        )
        session.add(contract)
        return contract

    def _import_excel(self, file_path: Path) -> int:
        """Import contracts from an LBB Excel export."""
        # Read Excel file
        df = pd.read_excel(file_path)

        # Standardize column names
        df.columns = [self._normalize_column(c) for c in df.columns]

        count = 0
        with get_session() as session:
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Importing contracts"):
                contract = self._create_contract(session, row)
                if contract:
                    count += 1

                if count % 500 == 0:
                    session.commit()

        return count

    def _normalize_column(self, column: str) -> str:
        """Normalize Excel column names."""
        # Convert to lowercase, replace spaces with underscores
        col = str(column).lower().strip()
        col = col.replace(" ", "_").replace("-", "_")
        col = "".join(c for c in col if c.isalnum() or c == "_")
        return col

    def _create_contract(self, session, row) -> Optional[Contract]:
        """Create a contract record from Excel row."""
        # Extract contract number
        contract_number = str(row.get("contract_number", row.get("contract_no", ""))).strip()
        if not contract_number or contract_number == "nan":
            return None

        # Check for existing
        existing = session.query(Contract).filter(
            Contract.contract_number == contract_number
        ).first()

        if existing:
            # Update existing contract
            contract = existing
        else:
            contract = Contract(contract_number=contract_number)
            session.add(contract)

        # Vendor
        vendor_name = str(row.get("vendor_name", row.get("contractor", ""))).strip()
        if vendor_name and vendor_name != "nan":
            vendor = self._get_or_create_vendor(session, vendor_name)
            contract.vendor_id = vendor.id if vendor else None

        # Agency
        agency_name = str(row.get("agency_name", row.get("agency", ""))).strip()
        if agency_name and agency_name != "nan":
            agency = self._get_or_create_agency(session, agency_name)
            contract.agency_id = agency.id if agency else None

        # Description
        description = str(row.get("description", row.get("contract_description", ""))).strip()
        if description and description != "nan":
            contract.description = description

        # Values
        for value_field in ["current_value", "contract_value", "value"]:
            if value_field in row:
                try:
                    val = row[value_field]
                    if pd.notna(val):
                        contract.current_value = Decimal(str(val).replace(",", "").replace("$", ""))
                        break
                except:
                    pass

        for max_field in ["max_value", "maximum_value", "max_contract_value"]:
            if max_field in row:
                try:
                    val = row[max_field]
                    if pd.notna(val):
                        contract.max_value = Decimal(str(val).replace(",", "").replace("$", ""))
                        break
                except:
                    pass

        # Dates
        for start_field in ["start_date", "effective_date", "begin_date"]:
            if start_field in row:
                try:
                    val = row[start_field]
                    if pd.notna(val):
                        if isinstance(val, str):
                            contract.start_date = datetime.strptime(val, "%m/%d/%Y").date()
                        else:
                            contract.start_date = val.date() if hasattr(val, 'date') else val
                        break
                except:
                    pass

        for end_field in ["end_date", "expiration_date", "termination_date"]:
            if end_field in row:
                try:
                    val = row[end_field]
                    if pd.notna(val):
                        if isinstance(val, str):
                            contract.end_date = datetime.strptime(val, "%m/%d/%Y").date()
                        else:
                            contract.end_date = val.date() if hasattr(val, 'date') else val
                        break
                except:
                    pass

        # NIGP codes
        nigp = str(row.get("nigp_code", row.get("commodity_code", ""))).strip()
        if nigp and nigp != "nan":
            contract.nigp_codes = [c.strip() for c in nigp.split(",")]

        # Fiscal year
        for fy_field in ["fiscal_year", "fy", "award_fiscal_year"]:
            if fy_field in row:
                try:
                    val = row[fy_field]
                    if pd.notna(val):
                        contract.fiscal_year = int(val)
                        break
                except:
                    pass

        # Source
        contract.source = "LBB"

        # Store raw data
        contract.raw_data = row.to_dict()

        return contract

    def _get_or_create_vendor(self, session, name: str) -> Optional[Vendor]:
        """Get or create vendor by name."""
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
        """Get or create agency by name."""
        if not name:
            return None

        agency = session.query(Agency).filter(Agency.name == name).first()

        if not agency:
            # Generate code from name
            code = "".join(word[0] for word in name.split()[:4]).upper()
            agency = Agency(agency_code=code, name=name)
            session.add(agency)
            session.flush()

        return agency


def import_lbb_excel(file_path: str) -> int:
    """
    Convenience function to import an LBB Excel file.

    Args:
        file_path: Path to Excel file

    Returns:
        Number of contracts imported
    """
    # Copy file to import directory
    from shutil import copy
    import_dir = config.data_dir / "lbb_imports"
    import_dir.mkdir(parents=True, exist_ok=True)

    dest = import_dir / Path(file_path).name
    copy(file_path, dest)

    # Run import
    ingestor = LBBIngestor()
    return ingestor.sync()


def get_lbb_thresholds() -> dict:
    """Get LBB reporting thresholds."""
    return LBBIngestor.THRESHOLDS
