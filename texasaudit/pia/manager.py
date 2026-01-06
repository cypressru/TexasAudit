"""PIA request management functionality."""

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from texasaudit.database import get_session, PIARequest, PIAStatus, Alert, Agency


class PIAManager:
    """Manages PIA request creation and tracking."""

    # Texas PIA response deadline is 10 business days
    RESPONSE_DAYS = 10

    def __init__(self):
        self.templates_dir = Path(__file__).parent / "templates"

    def create_draft(
        self,
        agency_id: Optional[int] = None,
        subject: str = "",
        request_text: str = "",
        alert_id: Optional[int] = None,
    ) -> int:
        """
        Create a draft PIA request.

        Returns:
            ID of created request.
        """
        with get_session() as session:
            request = PIARequest(
                agency_id=agency_id,
                subject=subject,
                request_text=request_text,
                related_alert_id=alert_id,
                status=PIAStatus.DRAFT,
            )
            session.add(request)
            session.flush()
            return request.id

    def create_from_alert(self, alert_id: int) -> int:
        """
        Create a PIA request draft based on an alert.

        Generates appropriate request text based on alert type and evidence.
        """
        with get_session() as session:
            alert = session.get(Alert, alert_id)
            if not alert:
                raise ValueError(f"Alert {alert_id} not found")

            # Determine agency from alert evidence or entity
            agency_id = None
            if alert.evidence and "agency_id" in alert.evidence:
                agency_id = alert.evidence["agency_id"]

            # Generate request text based on alert type
            request_text = self._generate_request_text(alert)

            request = PIARequest(
                agency_id=agency_id,
                subject=f"Records Request: {alert.title}",
                request_text=request_text,
                related_alert_id=alert_id,
                status=PIAStatus.DRAFT,
            )
            session.add(request)
            session.flush()
            return request.id

    def _generate_request_text(self, alert: Alert) -> str:
        """Generate PIA request text from alert details."""
        templates = {
            "contract_splitting": self._template_contract_splitting,
            "duplicate_payment": self._template_duplicate_payment,
            "vendor_clustering": self._template_vendor_relationship,
            "confidentiality": self._template_confidentiality,
        }

        template_func = templates.get(alert.alert_type, self._template_generic)
        return template_func(alert)

    def _template_generic(self, alert: Alert) -> str:
        """Generic PIA request template."""
        return f"""Dear Public Information Officer,

Pursuant to the Texas Public Information Act (Texas Government Code, Chapter 552), I am requesting access to and copies of the following records:

Subject: {alert.title}

{alert.description}

Please provide:
1. All documents, contracts, and correspondence related to this matter
2. Payment records and supporting documentation
3. Any internal communications regarding this matter

I am willing to pay reasonable copying costs. Please contact me if costs exceed $40.

This request is being made for public oversight purposes.

Thank you for your assistance.
"""

    def _template_contract_splitting(self, alert: Alert) -> str:
        """Template for contract splitting investigations."""
        evidence = alert.evidence or {}
        vendor_name = evidence.get("vendor_name", "[VENDOR NAME]")
        contracts = evidence.get("contracts", [])

        contract_list = "\n".join([
            f"  - Contract #{c.get('number', 'N/A')}: ${c.get('value', 0):,.2f}"
            for c in contracts[:10]
        ])

        return f"""Dear Public Information Officer,

Pursuant to the Texas Public Information Act (Texas Government Code, Chapter 552), I am requesting records related to potential contract splitting with the following vendor:

Vendor: {vendor_name}

Contracts of interest:
{contract_list}

Please provide:
1. Complete copies of all contracts listed above
2. All solicitation documents, bids received, and selection justifications
3. Documentation of any sole-source or emergency procurement justifications
4. Internal communications regarding these procurements
5. Any waivers of competitive bidding requirements

This request is made for public oversight purposes to ensure compliance with state procurement thresholds.

Thank you for your assistance.
"""

    def _template_duplicate_payment(self, alert: Alert) -> str:
        """Template for duplicate payment investigations."""
        evidence = alert.evidence or {}
        return f"""Dear Public Information Officer,

Pursuant to the Texas Public Information Act (Texas Government Code, Chapter 552), I am requesting records related to the following payments that appear to be duplicates:

{alert.description}

Please provide:
1. Complete payment documentation including invoices and approvals
2. Vendor correspondence related to these payments
3. Any recovery or refund documentation if applicable
4. Internal review or audit findings related to these payments

Thank you for your assistance.
"""

    def _template_vendor_relationship(self, alert: Alert) -> str:
        """Template for vendor relationship investigations."""
        return f"""Dear Public Information Officer,

Pursuant to the Texas Public Information Act (Texas Government Code, Chapter 552), I am requesting vendor registration and ownership records.

{alert.description}

Please provide:
1. Complete vendor registration files for the entities identified
2. Ownership disclosure documents
3. Conflict of interest disclosures
4. Any debarment or suspension records

Thank you for your assistance.
"""

    def _template_confidentiality(self, alert: Alert) -> str:
        """Template for confidentiality flag investigations."""
        return f"""Dear Public Information Officer,

Pursuant to the Texas Public Information Act (Texas Government Code, Chapter 552), I am requesting information about transactions marked as confidential.

{alert.description}

Please provide:
1. Legal justification for each confidentiality designation
2. Aggregate statistics on confidential transactions by category
3. Any policies governing confidentiality determinations

I understand that truly confidential information should be withheld, but I request that the legal basis for each withholding be provided.

Thank you for your assistance.
"""

    def mark_submitted(self, request_id: int, submitted_date: Optional[date] = None) -> None:
        """Mark a request as submitted and calculate due date."""
        with get_session() as session:
            request = session.get(PIARequest, request_id)
            if not request:
                raise ValueError(f"Request {request_id} not found")

            request.submitted_date = submitted_date or date.today()
            request.due_date = self._calculate_due_date(request.submitted_date)
            request.status = PIAStatus.PENDING

    def _calculate_due_date(self, start_date: date) -> date:
        """Calculate due date (10 business days from submission)."""
        business_days = 0
        current = start_date

        while business_days < self.RESPONSE_DAYS:
            current += timedelta(days=1)
            # Skip weekends (0 = Monday, 6 = Sunday)
            if current.weekday() < 5:
                business_days += 1

        return current

    def check_overdue(self) -> list[int]:
        """Find and mark overdue requests."""
        with get_session() as session:
            overdue = session.query(PIARequest).filter(
                PIARequest.status == PIAStatus.PENDING,
                PIARequest.due_date < date.today(),
            ).all()

            overdue_ids = []
            for request in overdue:
                request.status = PIAStatus.OVERDUE
                overdue_ids.append(request.id)

            return overdue_ids


def create_draft(
    alert_id: Optional[int] = None,
    agency_code: Optional[str] = None,
    subject: Optional[str] = None,
) -> int:
    """
    Convenience function to create a PIA request draft.

    Use alert_id to create from an alert, or provide agency_code and subject.
    """
    manager = PIAManager()

    if alert_id:
        return manager.create_from_alert(alert_id)

    # Look up agency ID from code
    agency_id = None
    if agency_code:
        with get_session() as session:
            agency = session.query(Agency).filter(
                Agency.agency_code == agency_code
            ).first()
            if agency:
                agency_id = agency.id

    return manager.create_draft(
        agency_id=agency_id,
        subject=subject or "",
    )
