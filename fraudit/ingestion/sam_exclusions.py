"""
SAM.gov Exclusions ingestor.

Downloads the federal exclusions list from SAM.gov which contains
entities debarred, suspended, or otherwise excluded from federal
procurement and nonprocurement programs.

SAM.gov now requires an API key for data access.
Register at: https://sam.gov/data-services/

Alternative: Manual download from https://sam.gov/data-services/
Look for "Exclusions Extract" under Entity/Exclusions Data
"""

import csv
import io
import json
import os
import zipfile
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

from fraudit.config import config
from fraudit.database import get_session, DebarredEntity
from fraudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class SAMExclusionsIngestor(BaseIngestor):
    """Ingestor for SAM.gov federal exclusions list."""

    source_name = "sam_exclusions"

    # SAM.gov API endpoint (requires API key)
    API_URL = "https://api.sam.gov/entity-information/v3/exclusions"

    # Page size for API requests
    PAGE_SIZE = 1000

    def __init__(self):
        super().__init__()
        self.data_dir = Path(config.data_dir)
        self.api_key = self._get_api_key()

    def _get_api_key(self) -> Optional[str]:
        """Get SAM.gov API key from config or environment."""
        # Check environment variable first
        api_key = os.environ.get("SAM_API_KEY")
        if api_key:
            return api_key

        # Check config
        api_keys = getattr(config, 'api_keys', {})
        if isinstance(api_keys, dict):
            return api_keys.get("sam_gov")

        return None

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Download and import SAM.gov exclusions data.

        Args:
            since: Not used (full refresh each time)

        Returns:
            Number of exclusion records synced
        """
        # First, check for manually downloaded CSV file
        manual_file = self.data_dir / "SAM_Exclusions.csv"
        manual_zip = self.data_dir / "SAM_Exclusions_Public_Extract_V2.ZIP"

        if manual_zip.exists():
            print(f"Found manual download: {manual_zip}")
            csv_data = self._extract_zip(manual_zip)
            if csv_data:
                print(f"Processing {len(csv_data):,} exclusion records from ZIP...")
                return self._import_exclusions(csv_data)

        if manual_file.exists():
            print(f"Found manual download: {manual_file}")
            csv_data = self._load_csv(manual_file)
            if csv_data:
                print(f"Processing {len(csv_data):,} exclusion records from CSV...")
                return self._import_exclusions(csv_data)

        # Try API if we have a key
        if self.api_key:
            print("Using SAM.gov API...")
            try:
                return self._sync_via_api()
            except Exception as e:
                print(f"API sync failed: {e}")
                print("Falling back to manual download instructions...")

        # No API key and no manual file
        print("\n" + "="*70)
        print("SAM.gov requires an API key for automated downloads.")
        print("="*70)
        print("\nOption 1: Get an API key (recommended)")
        print("  1. Go to https://sam.gov/data-services/")
        print("  2. Click 'API Key Request' and register")
        print("  3. Set environment variable: export SAM_API_KEY=your_key")
        print("  4. Or add to config.yaml:")
        print("     api_keys:")
        print("       sam_gov: \"your_key_here\"")
        print("\nOption 2: Manual download")
        print("  1. Go to https://sam.gov/data-services/")
        print("  2. Download 'SAM_Exclusions_Public_Extract_V2.ZIP'")
        print(f"  3. Save to: {self.data_dir}/")
        print("  4. Run sync again")
        print("="*70 + "\n")

        raise Exception("SAM.gov API key required. See instructions above.")

    def _sync_via_api(self) -> int:
        """Sync exclusions via SAM.gov API."""
        print("Fetching exclusions from SAM.gov API...")

        all_records = []
        page = 0
        total_pages = None

        with tqdm(desc="Downloading exclusions") as pbar:
            while True:
                params = {
                    "api_key": self.api_key,
                    "page": page,
                    "size": self.PAGE_SIZE,
                    "includeSections": "exclusionDetails,exclusionIdentification,exclusionActions,exclusionAddress",
                }

                response = requests.get(self.API_URL, params=params, timeout=60)
                response.raise_for_status()

                data = response.json()

                if "results" not in data:
                    if "error" in data:
                        raise Exception(f"API Error: {data['error']}")
                    break

                results = data["results"]
                if not results:
                    break

                all_records.extend(results)
                pbar.update(len(results))

                # Check pagination
                if total_pages is None:
                    total_records = data.get("totalRecords", len(results))
                    total_pages = (total_records + self.PAGE_SIZE - 1) // self.PAGE_SIZE
                    pbar.total = total_records

                page += 1
                if page >= total_pages:
                    break

        print(f"\nDownloaded {len(all_records):,} exclusion records")

        if not all_records:
            return 0

        return self._import_api_records(all_records)

    def _import_api_records(self, records: list[dict]) -> int:
        """Import records from API format."""
        count = 0
        updated = 0

        with get_session() as session:
            existing_sam = {
                e.sam_number: e for e in
                session.query(DebarredEntity).filter(
                    DebarredEntity.source == "sam_gov",
                    DebarredEntity.sam_number.isnot(None)
                ).all()
            }

            for record in tqdm(records, desc="Importing"):
                # API returns nested structure
                sam_number = record.get("samNumber") or record.get("exclusionIdentification", {}).get("samNumber")
                if not sam_number:
                    continue

                # Extract fields from API response
                identification = record.get("exclusionIdentification", {})
                details = record.get("exclusionDetails", {})
                address_info = record.get("exclusionAddress", {})
                actions = record.get("exclusionActions", {})

                entity_name = identification.get("name") or ""
                if not entity_name:
                    # Try individual name parts
                    parts = [
                        identification.get("prefix", ""),
                        identification.get("firstName", ""),
                        identification.get("middleName", ""),
                        identification.get("lastName", ""),
                        identification.get("suffix", ""),
                    ]
                    entity_name = " ".join(p for p in parts if p).strip() or "Unknown"

                name_normalized = normalize_vendor_name(entity_name)

                # Parse dates
                start_date = self._parse_date(actions.get("activeDate", ""))
                end_date = self._parse_date(actions.get("terminationDate", ""))

                is_active = True
                if end_date and end_date < date.today():
                    is_active = False

                # Build address
                address_parts = [
                    address_info.get("addressLine1", ""),
                    address_info.get("addressLine2", ""),
                ]
                address = ", ".join(p for p in address_parts if p) or None

                if sam_number in existing_sam:
                    existing = existing_sam[sam_number]
                    existing.entity_name = entity_name
                    existing.name_normalized = name_normalized
                    existing.classification = details.get("classification")
                    existing.exclusion_type = details.get("exclusionType")
                    existing.exclusion_program = details.get("exclusionProgram")
                    existing.excluding_agency = actions.get("excludingAgency")
                    existing.cage_code = identification.get("cageCode")
                    existing.uei = identification.get("ueiSAM")
                    existing.address = address
                    existing.city = address_info.get("city")
                    existing.state = address_info.get("stateOrProvinceCode")
                    existing.zip_code = address_info.get("zipCode")
                    existing.country = address_info.get("countryCode")
                    existing.start_date = start_date
                    existing.end_date = end_date
                    existing.is_active = is_active
                    existing.reason = actions.get("additionalComments")
                    existing.raw_data = record
                    updated += 1
                else:
                    exclusion = DebarredEntity(
                        source="sam_gov",
                        sam_number=sam_number,
                        entity_name=entity_name,
                        name_normalized=name_normalized,
                        classification=details.get("classification"),
                        exclusion_type=details.get("exclusionType"),
                        exclusion_program=details.get("exclusionProgram"),
                        excluding_agency=actions.get("excludingAgency"),
                        cage_code=identification.get("cageCode"),
                        uei=identification.get("ueiSAM"),
                        address=address,
                        city=address_info.get("city"),
                        state=address_info.get("stateOrProvinceCode"),
                        zip_code=address_info.get("zipCode"),
                        country=address_info.get("countryCode"),
                        start_date=start_date,
                        end_date=end_date,
                        is_active=is_active,
                        reason=actions.get("additionalComments"),
                        raw_data=record,
                    )
                    session.add(exclusion)
                    count += 1

                if (count + updated) % 1000 == 0:
                    session.commit()

            session.commit()

        print(f"  New: {count:,}, Updated: {updated:,}")
        return count + updated

    def _extract_zip(self, zip_path: Path) -> list[dict]:
        """Extract CSV from ZIP file."""
        with zipfile.ZipFile(zip_path) as zf:
            csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                raise Exception("No CSV file found in ZIP")

            with zf.open(csv_files[0]) as f:
                try:
                    content = f.read().decode('utf-8')
                except UnicodeDecodeError:
                    f.seek(0)
                    content = f.read().decode('latin-1')

                reader = csv.DictReader(io.StringIO(content))
                return list(reader)

    def _load_csv(self, csv_path: Path) -> list[dict]:
        """Load CSV file directly."""
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _import_exclusions(self, records: list[dict]) -> int:
        """
        Import exclusion records from CSV format into database.

        SAM.gov V2 CSV columns:
        - SAM Number
        - Name / Prefix / First / Middle / Last / Suffix
        - Classification
        - Exclusion Type
        - Exclusion Program
        - Excluding Agency
        - CT Code
        - UEI
        - Active Date
        - Termination Date
        - Additional Comments
        - Address 1-4
        - City, State Province, Zip, Country
        """
        count = 0
        updated = 0
        skipped = 0

        with get_session() as session:
            existing_sam = {
                e.sam_number: e for e in
                session.query(DebarredEntity).filter(
                    DebarredEntity.source == "sam_gov",
                    DebarredEntity.sam_number.isnot(None)
                ).all()
            }

            # Track SAM numbers we've already processed in this batch
            seen_sam_numbers = set(existing_sam.keys())

            for record in tqdm(records, desc="Importing SAM exclusions"):
                sam_number = record.get("SAM Number", "").strip()
                if not sam_number:
                    skipped += 1
                    continue

                # Skip duplicates within the CSV file
                if sam_number in seen_sam_numbers and sam_number not in existing_sam:
                    skipped += 1
                    continue

                # Build entity name
                name_parts = []
                if record.get("Name"):
                    name_parts.append(record["Name"].strip())
                else:
                    for part in ["Prefix", "First", "Middle", "Last", "Suffix"]:
                        if record.get(part):
                            name_parts.append(record[part].strip())

                entity_name = " ".join(name_parts) if name_parts else "Unknown"
                name_normalized = normalize_vendor_name(entity_name)

                start_date = self._parse_date(record.get("Active Date", ""))
                end_date = self._parse_date(record.get("Termination Date", ""))

                is_active = True
                if end_date and end_date < date.today():
                    is_active = False

                address_parts = []
                for i in range(1, 5):
                    addr = record.get(f"Address {i}", "").strip()
                    if addr:
                        address_parts.append(addr)
                address = ", ".join(address_parts) if address_parts else None

                if sam_number in existing_sam:
                    existing = existing_sam[sam_number]
                    existing.entity_name = entity_name
                    existing.name_normalized = name_normalized
                    existing.classification = record.get("Classification", "").strip() or None
                    existing.exclusion_type = record.get("Exclusion Type", "").strip() or None
                    existing.exclusion_program = record.get("Exclusion Program", "").strip() or None
                    existing.excluding_agency = record.get("Excluding Agency", "").strip() or None
                    existing.cage_code = record.get("CT Code", "").strip() or None
                    existing.uei = record.get("UEI", "").strip() or None
                    existing.address = address
                    existing.city = record.get("City", "").strip() or None
                    existing.state = record.get("State Province", "").strip() or None
                    existing.zip_code = record.get("Zip", "").strip() or None
                    existing.country = record.get("Country", "").strip() or None
                    existing.start_date = start_date
                    existing.end_date = end_date
                    existing.is_active = is_active
                    existing.reason = record.get("Additional Comments", "").strip() or None
                    existing.raw_data = record
                    updated += 1
                else:
                    exclusion = DebarredEntity(
                        source="sam_gov",
                        sam_number=sam_number,
                        entity_name=entity_name,
                        name_normalized=name_normalized,
                        classification=record.get("Classification", "").strip() or None,
                        exclusion_type=record.get("Exclusion Type", "").strip() or None,
                        exclusion_program=record.get("Exclusion Program", "").strip() or None,
                        excluding_agency=record.get("Excluding Agency", "").strip() or None,
                        cage_code=record.get("CT Code", "").strip() or None,
                        uei=record.get("UEI", "").strip() or None,
                        address=address,
                        city=record.get("City", "").strip() or None,
                        state=record.get("State Province", "").strip() or None,
                        zip_code=record.get("Zip", "").strip() or None,
                        country=record.get("Country", "").strip() or None,
                        start_date=start_date,
                        end_date=end_date,
                        is_active=is_active,
                        reason=record.get("Additional Comments", "").strip() or None,
                        raw_data=record,
                    )
                    session.add(exclusion)
                    seen_sam_numbers.add(sam_number)
                    count += 1

                if (count + updated) % 1000 == 0:
                    session.commit()

            session.commit()

        print(f"  New: {count:,}, Updated: {updated:,}, Skipped: {skipped:,}")
        return count + updated

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string from SAM.gov format."""
        if not date_str:
            return None

        date_str = str(date_str).strip()

        for fmt in [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y%m%d",
            "%d-%b-%Y",
            "%d %b %Y",
        ]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None


def get_active_exclusions_count() -> int:
    """Get count of currently active exclusions."""
    with get_session() as session:
        return session.query(DebarredEntity).filter(
            DebarredEntity.source == "sam_gov",
            DebarredEntity.is_active == True
        ).count()


def check_vendor_against_exclusions(vendor_name: str, threshold: float = 0.90) -> list[dict]:
    """
    Check if a vendor name matches any excluded entities.

    Args:
        vendor_name: Name to check
        threshold: Fuzzy match threshold (0-1)

    Returns:
        List of matching exclusions with match scores
    """
    from rapidfuzz import fuzz

    normalized = normalize_vendor_name(vendor_name)
    matches = []

    with get_session() as session:
        exclusions = session.query(DebarredEntity).filter(
            DebarredEntity.is_active == True
        ).all()

        for exc in exclusions:
            if exc.name_normalized == normalized:
                matches.append({
                    "exclusion": exc,
                    "match_type": "exact",
                    "score": 1.0,
                })
                continue

            score = fuzz.ratio(normalized, exc.name_normalized or "") / 100.0
            if score >= threshold:
                matches.append({
                    "exclusion": exc,
                    "match_type": "fuzzy",
                    "score": score,
                })

    return sorted(matches, key=lambda x: x["score"], reverse=True)
