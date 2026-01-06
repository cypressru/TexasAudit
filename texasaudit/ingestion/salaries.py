"""
Texas state employee salary data ingestion.

Downloads employee salary data from the Texas Tribune's Government Salaries Explorer.
The Texas Tribune obtains this data quarterly via Texas Public Information Act requests
to the state comptroller.

Source: https://salaries.texastribune.org/
Data updates: Quarterly (January, April, July, October)
"""

import csv
import io
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Optional

import requests
from tqdm import tqdm

from texasaudit.config import config
from texasaudit.database import get_session, Agency, Employee
from texasaudit.normalization import normalize_vendor_name
from .base import BaseIngestor


class SalariesIngestor(BaseIngestor):
    """Ingestor for Texas state employee salary data."""

    source_name = "employee_salaries"

    # Texas Tribune salary data download URL (S3 bucket)
    # Updated quarterly - format: YYYY-MM-01.csv
    DOWNLOAD_URL = "https://s3.amazonaws.com/raw.texastribune.org/state_of_texas/salaries/02_non_duplicated_employees/2025-10-01.csv"

    def __init__(self):
        super().__init__()
        self.data_dir = config.data_dir

    def _do_sync(self, since: Optional[datetime] = None) -> int:
        """
        Download and import employee salary data.

        Note: The Texas Tribune updates data quarterly, so incremental sync
        by timestamp may not be meaningful. We do a full download and upsert.

        Args:
            since: Not used for this ingestor (quarterly full updates)

        Returns:
            Number of employee records synced
        """
        print("Downloading Texas state employee salary data...")

        # Download CSV data
        try:
            csv_data = self._download_salary_csv()
        except Exception as e:
            print(f"Error downloading salary data: {e}")
            print("Please verify the download URL is correct.")
            print("Manual download available at: https://salaries.texastribune.org/")
            raise

        # Parse and import
        print(f"Processing {len(csv_data)} employee records...")
        return self._import_employees(csv_data)

    def _download_salary_csv(self) -> list[dict]:
        """
        Download salary CSV from Texas Tribune.

        Returns:
            List of employee record dictionaries

        Raises:
            requests.RequestException: If download fails
        """
        # Try the download URL
        try:
            response = requests.get(self.DOWNLOAD_URL, timeout=120)
            response.raise_for_status()
        except requests.RequestException as e:
            # If direct download fails, provide helpful error message
            raise Exception(
                f"Failed to download from {self.DOWNLOAD_URL}. "
                "You may need to manually download the CSV from "
                "https://salaries.texastribune.org/ and update the DOWNLOAD_URL. "
                f"Original error: {e}"
            )

        # Handle encoding - likely UTF-8 but may be Latin-1
        try:
            content = response.content.decode("utf-8")
        except UnicodeDecodeError:
            content = response.content.decode("latin-1")

        # Parse CSV
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _import_employees(self, records: list[dict]) -> int:
        """
        Import employee records into database.

        Expected CSV fields (based on Texas Tribune format):
        - Name
        - Title (job title)
        - Department (agency name)
        - Race
        - Gender
        - Status (full time/part time)
        - Hire Date
        - Gross Annual Salary

        Args:
            records: List of employee record dictionaries from CSV

        Returns:
            Number of records imported
        """
        count = 0

        with get_session() as session:
            for record in tqdm(records, desc="Importing employees"):
                # Build full name from FIRST NAME + LAST NAME columns
                first_name = record.get("FIRST NAME", "").strip()
                last_name = record.get("LAST NAME", "").strip()

                if not first_name and not last_name:
                    # Try alternate column names
                    name = record.get("Name", "").strip()
                else:
                    name = f"{first_name} {last_name}".strip()

                if not name:
                    continue

                # Normalize name for deduplication
                name_normalized = normalize_vendor_name(name)

                # Extract agency/department (AGENCY NAME column)
                agency_name = (
                    record.get("AGENCY NAME", "") or
                    record.get("Agency Name", "") or
                    record.get("Department", "") or
                    record.get("agency", "")
                ).strip()

                agency = None
                if agency_name:
                    agency = self._get_or_create_agency(session, agency_name, record)

                # Extract job title (CLASS TITLE column)
                job_title = (
                    record.get("CLASS TITLE", "") or
                    record.get("Class Title", "") or
                    record.get("Title", "") or
                    record.get("Job Title", "")
                ).strip()

                # Extract and parse salary (ANNUAL column)
                salary_str = (
                    record.get("ANNUAL", "") or
                    record.get("Annual", "") or
                    record.get("summed_annual_salary", "") or
                    record.get("Salary", "")
                ).strip()

                annual_salary = None
                if salary_str:
                    try:
                        # Remove currency symbols and commas
                        cleaned = salary_str.replace("$", "").replace(",", "").strip()
                        annual_salary = Decimal(cleaned)
                    except (InvalidOperation, ValueError):
                        pass  # Skip invalid salary values

                # Extract hire date (HIRE DATE column)
                hire_date_str = (
                    record.get("HIRE DATE", "") or
                    record.get("Hire Date", "") or
                    record.get("hire_date", "")
                ).strip()

                hire_date = None
                if hire_date_str:
                    hire_date = self._parse_date(hire_date_str)

                # Extract employment status (EMPLOYEE TYPE column)
                employment_status = (
                    record.get("EMPLOYEE TYPE", "") or
                    record.get("Employee Type", "") or
                    record.get("Status", "")
                ).strip()

                # Check for existing employee by normalized name + agency
                existing = session.query(Employee).filter(
                    Employee.name_normalized == name_normalized,
                    Employee.agency_id == (agency.id if agency else None),
                ).first()

                if existing:
                    # Update existing record
                    existing.job_title = job_title
                    existing.annual_salary = annual_salary
                    existing.hire_date = hire_date
                    existing.employment_status = employment_status
                    existing.raw_data = record
                else:
                    # Create new employee record
                    employee = Employee(
                        name=name,
                        name_normalized=name_normalized,
                        agency_id=agency.id if agency else None,
                        job_title=job_title,
                        annual_salary=annual_salary,
                        hire_date=hire_date,
                        employment_status=employment_status,
                        raw_data=record,
                    )
                    session.add(employee)

                count += 1

                # Commit in batches for performance
                if count % 1000 == 0:
                    session.commit()

        return count

    def _get_or_create_agency(self, session, name: str, record: dict) -> Optional[Agency]:
        """
        Get existing agency or create new one.

        Args:
            session: Database session
            name: Agency name
            record: Raw record data

        Returns:
            Agency object or None
        """
        if not name:
            return None

        # Try to extract agency code if available (AGENCY column has the code)
        agency_code = (
            record.get("AGENCY", "") or
            record.get("agency_code", "") or
            record.get("Agency Code", "")
        ).strip()

        if not agency_code:
            # Generate code from name (first letters of first 4 words)
            agency_code = "".join(word[0] for word in name.split()[:4]).upper()

        # Truncate to fit database column
        agency_code = agency_code[:20]

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

    def _parse_date(self, date_str: str) -> Optional[date]:
        """
        Parse date string into date object.

        Handles multiple common date formats:
        - YYYY-MM-DD
        - MM/DD/YYYY
        - YYYY-MM-DDTHH:MM:SS (ISO format)

        Args:
            date_str: Date string to parse

        Returns:
            date object or None if parsing fails
        """
        if not date_str:
            return None

        # Try various date formats
        for fmt in [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%m-%d-%Y",
            "%Y/%m/%d",
        ]:
            try:
                # Handle ISO format with timezone
                if "T" in date_str:
                    date_str = date_str.split("T")[0]
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None


# Additional helper functions for salary data analysis

def get_agency_salary_stats(agency_code: str) -> dict:
    """
    Get salary statistics for a specific agency.

    Args:
        agency_code: Agency code to analyze

    Returns:
        Dictionary with salary statistics (min, max, avg, median, count)
    """
    # TODO: Implement once Employee model exists
    # from texasaudit.database import Employee
    # with get_session() as session:
    #     agency = session.query(Agency).filter(
    #         Agency.agency_code == agency_code
    #     ).first()
    #
    #     if not agency:
    #         return {}
    #
    #     employees = session.query(Employee).filter(
    #         Employee.agency_id == agency.id
    #     ).all()
    #
    #     salaries = [e.annual_salary for e in employees if e.annual_salary]
    #
    #     if not salaries:
    #         return {"count": 0}
    #
    #     return {
    #         "count": len(salaries),
    #         "min": min(salaries),
    #         "max": max(salaries),
    #         "avg": sum(salaries) / len(salaries),
    #         "median": sorted(salaries)[len(salaries) // 2],
    #     }
    pass


def search_employees_by_name(name: str, limit: int = 100) -> list:
    """
    Search for employees by name.

    Args:
        name: Name or partial name to search
        limit: Maximum results to return

    Returns:
        List of Employee objects
    """
    # TODO: Implement once Employee model exists
    # from texasaudit.database import Employee
    # normalized = normalize_vendor_name(name)
    #
    # with get_session() as session:
    #     employees = session.query(Employee).filter(
    #         Employee.name_normalized.ilike(f"%{normalized}%")
    #     ).limit(limit).all()
    #
    #     return employees
    pass
