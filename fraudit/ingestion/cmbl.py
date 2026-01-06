"""
CMBL (Centralized Master Bidders List) vendor data ingestion.

Downloads vendor CSV files from the Texas Comptroller and imports them.
These files update within 30 minutes of profile changes - excellent for monitoring.

Source: https://comptroller.texas.gov/purchasing/downloads/
"""

import csv
import io
from datetime import datetime
from typing import Optional

import requests
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, Vendor
from fraudit.normalization import normalize_vendor_name, normalize_address
from .base import BaseIngestor


class CMBLIngestor(BaseIngestor):
    """Ingestor for CMBL vendor data."""

    source_name = "cmbl"

    # CMBL CSV download URLs
    BASE_URL = "https://comptroller.texas.gov"
    VENDOR_CSV = "/auto-data/purchasing/web_name.csv"
    HUB_CSV = "/auto-data/purchasing/hub_name.csv"
    VENDOR_CLASS_CSV = "/auto-data/purchasing/vnr_clas.csv"

    def __init__(self):
        super().__init__()
        self.data_dir = config.data_dir

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Download and import CMBL vendor data.

        Note: CMBL CSVs don't support incremental sync by timestamp,
        so we always do a full download and upsert.
        """
        total_records = 0

        # Download and process main vendor file
        print("Downloading CMBL vendor data...")
        total_records += self._import_vendors()

        # Download and process HUB file
        print("Downloading HUB vendor data...")
        total_records += self._import_hub_vendors()

        # Download vendor class/NIGP codes
        print("Downloading vendor class data...")
        self._import_vendor_classes()

        return total_records

    def _download_csv(self, path: str) -> list[dict]:
        """Download a CSV file and return as list of dicts."""
        url = f"{self.BASE_URL}{path}"
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        # Handle encoding - these files are typically Latin-1
        content = response.content.decode("latin-1")

        # Parse CSV
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _import_vendors(self) -> int:
        """Import main vendor file."""
        rows = self._download_csv(self.VENDOR_CSV)
        count = 0

        with get_session() as session:
            for row in tqdm(rows, desc="Importing vendors"):
                vendor = self._upsert_vendor(session, row, is_hub=False)
                if vendor:
                    count += 1

                # Commit in batches
                if count % 1000 == 0:
                    session.commit()

        return count

    def _import_hub_vendors(self) -> int:
        """Import HUB (Historically Underutilized Business) vendor file."""
        rows = self._download_csv(self.HUB_CSV)
        count = 0

        with get_session() as session:
            for row in tqdm(rows, desc="Importing HUB vendors"):
                vendor = self._upsert_vendor(session, row, is_hub=True)
                if vendor:
                    count += 1

                if count % 1000 == 0:
                    session.commit()

        return count

    def _import_vendor_classes(self) -> None:
        """Import vendor NIGP class codes."""
        rows = self._download_csv(self.VENDOR_CLASS_CSV)

        # Group by vendor ID - handle both old and new column formats
        vendor_codes = {}
        for row in rows:
            vid = (
                row.get("CLASS_VID", "") or
                row.get("VendorId", "")
            ).strip()
            nigp = (
                row.get("CLASS_CODE", "") or
                row.get("Class", "")
            ).strip()
            if vid and nigp:
                if vid not in vendor_codes:
                    vendor_codes[vid] = []
                vendor_codes[vid].append(nigp)

        # Update vendors with NIGP codes
        with get_session() as session:
            for vid, codes in tqdm(vendor_codes.items(), desc="Updating NIGP codes"):
                vendor = session.query(Vendor).filter(
                    Vendor.vendor_id == vid
                ).first()
                if vendor:
                    vendor.nigp_codes = list(set(codes))

    def _upsert_vendor(self, session, row: dict, is_hub: bool) -> Optional[Vendor]:
        """Insert or update a vendor from CSV row."""
        # Field mappings - handle multiple CSV formats:
        # - web_name.csv uses WEB_ prefix (WEB_VID, WEB_NAME_VENDOR_NAME, etc.)
        # - hub_name.csv uses spaces (VENDOR ID NUMBER, VENDOR NAME, etc.)
        vid = (
            row.get("WEB_VID", "") or
            row.get("VENDOR ID NUMBER", "") or
            row.get("VendorId", "") or
            row.get("Vendor_Id", "")
        ).strip()
        if not vid:
            return None

        name = (
            row.get("WEB_NAME_VENDOR_NAME", "") or
            row.get("VENDOR NAME", "") or
            row.get(" VENDOR NAME", "") or  # Some CSVs have leading space
            row.get("Name", "") or
            row.get("Vendor_Name", "")
        ).strip()
        if not name:
            return None

        # Check for existing vendor
        vendor = session.query(Vendor).filter(Vendor.vendor_id == vid).first()

        if not vendor:
            vendor = Vendor(vendor_id=vid)
            session.add(vendor)

        # Update fields
        vendor.name = name
        vendor.name_normalized = normalize_vendor_name(name)

        # Address fields - handle multiple formats
        address = (
            row.get("WEB_ADDR1", "") or
            row.get("VENDOR ADDRESS LINE 1", "") or
            row.get(" VENDOR ADDRESS LINE 1", "") or
            row.get("Address", "") or
            row.get("Street1", "")
        ).strip()
        city = (
            row.get("WEB_CITY", "") or
            row.get("CITY", "") or
            row.get("City", "")
        ).strip()
        state = (
            row.get("WEB_STATE", "") or
            row.get("STATE", "") or
            row.get("State", "")
        ).strip()
        zip_code = (
            row.get("WEB_ZIP", "") or
            row.get("ZIP CODE", "") or
            row.get("Zip", "") or
            row.get("ZipCode", "")
        ).strip()

        if address:
            parsed = normalize_address(address, city, state, zip_code)
            vendor.address = address
            vendor.city = parsed.city or city
            vendor.state = parsed.state or state
            vendor.zip_code = parsed.zip_code or zip_code

        # Phone
        phone = (
            row.get("WEB_PHONE", "") or
            row.get("PHONE NUMBER", "") or
            row.get(" PHONE NUMBER", "") or
            row.get("Phone", "") or
            row.get("PhoneNumber", "")
        ).strip()
        if phone:
            vendor.phone = phone

        # HUB status - from either file format
        hub_status = (
            row.get("WEB_HUB_STATUS", "") or
            row.get(" STATUS CODE", "") or
            row.get("STATUS CODE", "") or
            row.get("CertType", "") or
            row.get("Hub_Status", "")
        ).strip()
        if is_hub or hub_status:
            vendor.hub_status = hub_status if hub_status else "HUB"

        # Mark as in CMBL
        vendor.in_cmbl = True

        # Track first/last seen
        today = datetime.now().date()
        if not vendor.first_seen:
            vendor.first_seen = today
        vendor.last_seen = today

        # Store raw data
        vendor.raw_data = dict(row)

        return vendor


# Additional helper functions for CMBL data

def download_commodity_book() -> list[dict]:
    """
    Download the NIGP commodity code reference book.

    Returns list of dicts with Class, Item, Description fields.
    """
    ingestor = CMBLIngestor()
    url = f"{ingestor.BASE_URL}/auto-data/purchasing/comm_book.csv"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("latin-1")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def get_nigp_description(class_code: str, item_code: str = None) -> Optional[str]:
    """Look up NIGP code description from commodity book."""
    # Cache this in practice
    book = download_commodity_book()

    for row in book:
        if row.get("Class") == class_code:
            if item_code is None or row.get("Item") == item_code:
                return row.get("Description", "")

    return None
