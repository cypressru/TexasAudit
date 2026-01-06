"""
Contract Splitting Detection.

Detects potential contract splitting to avoid procurement thresholds:
- LBB reporting: $50,000
- ESBD posting: $25,000
- Professional services: $14,000

Flags vendors with multiple contracts clustered just below thresholds.
"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func

from fraudit.database import get_session, Contract, Vendor, Agency
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run contract splitting detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    # Get threshold parameters
    min_amount = Decimal(str(thresholds.get("contract_splitting_min", 45000)))
    max_amount = Decimal(str(thresholds.get("contract_splitting_max", 50000)))
    count_threshold = thresholds.get("contract_splitting_count", 3)
    months = thresholds.get("contract_splitting_months", 12)

    print(f"  Checking for contracts between ${min_amount:,.0f} and ${max_amount:,.0f}")
    print(f"  Flagging vendors with {count_threshold}+ contracts in {months} month window")

    # Also check ESBD threshold
    esbd_min = Decimal("22000")
    esbd_max = Decimal(str(thresholds.get("esbd_threshold", 25000)))

    with get_session() as session:
        # Query contracts in threshold range
        alerts_created += _check_threshold_range(
            session, min_amount, max_amount, count_threshold, months,
            "LBB reporting threshold ($50K)", "high"
        )

        # Check ESBD threshold too
        alerts_created += _check_threshold_range(
            session, esbd_min, esbd_max, count_threshold, months,
            "ESBD posting threshold ($25K)", "medium"
        )

    return alerts_created


def _check_threshold_range(
    session,
    min_amount: Decimal,
    max_amount: Decimal,
    count_threshold: int,
    months: int,
    threshold_name: str,
    severity: str,
) -> int:
    """Check a specific threshold range for splitting patterns."""
    alerts_created = 0

    # Find vendors with multiple contracts in the range
    cutoff_date = date.today() - timedelta(days=months * 30)

    # Query to find vendor/agency pairs with multiple contracts in range
    results = session.query(
        Contract.vendor_id,
        Contract.agency_id,
        func.count(Contract.id).label("contract_count"),
        func.sum(Contract.current_value).label("total_value"),
        func.avg(Contract.current_value).label("avg_value"),
    ).filter(
        Contract.current_value >= min_amount,
        Contract.current_value <= max_amount,
        Contract.start_date >= cutoff_date,
        Contract.vendor_id.isnot(None),
    ).group_by(
        Contract.vendor_id,
        Contract.agency_id,
    ).having(
        func.count(Contract.id) >= count_threshold
    ).all()

    for result in results:
        vendor = session.get(Vendor, result.vendor_id)
        agency = session.get(Agency, result.agency_id) if result.agency_id else None

        if not vendor:
            continue

        # Get the specific contracts
        contracts = session.query(Contract).filter(
            Contract.vendor_id == result.vendor_id,
            Contract.agency_id == result.agency_id,
            Contract.current_value >= min_amount,
            Contract.current_value <= max_amount,
            Contract.start_date >= cutoff_date,
        ).all()

        # Build evidence
        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "vendor_vid": vendor.vendor_id,
            "agency_id": result.agency_id,
            "agency_name": agency.name if agency else "Unknown",
            "contract_count": result.contract_count,
            "total_value": float(result.total_value) if result.total_value else 0,
            "average_value": float(result.avg_value) if result.avg_value else 0,
            "threshold_name": threshold_name,
            "contracts": [
                {
                    "number": c.contract_number,
                    "value": float(c.current_value) if c.current_value else 0,
                    "start_date": c.start_date.isoformat() if c.start_date else None,
                    "description": c.description[:100] if c.description else None,
                }
                for c in contracts
            ],
        }

        # Calculate how suspicious this is
        # Higher count = more suspicious
        # More uniform values = more suspicious
        values = [float(c.current_value) for c in contracts if c.current_value]
        if len(values) > 1:
            # Check coefficient of variation (low = uniform = suspicious)
            mean_val = sum(values) / len(values)
            std_dev = (sum((v - mean_val) ** 2 for v in values) / len(values)) ** 0.5
            cv = std_dev / mean_val if mean_val > 0 else 1
            evidence["coefficient_of_variation"] = round(cv, 3)

            # Very uniform values (CV < 0.1) are more suspicious
            if cv < 0.1 and result.contract_count >= 5:
                severity = "high"

        # Create alert
        agency_str = f" with {agency.name}" if agency else ""
        alert_id = create_alert(
            alert_type="contract_splitting",
            severity=severity,
            title=f"Potential contract splitting: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has {result.contract_count} contracts "
                f"in the ${min_amount:,.0f}-${max_amount:,.0f} range{agency_str} "
                f"within the past {months} months. "
                f"Total value: ${result.total_value:,.2f}. "
                f"This pattern may indicate intentional splitting to avoid "
                f"the {threshold_name}."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def get_splitting_candidates(
    min_contracts: int = 2,
    threshold: Decimal = Decimal("50000"),
    buffer: Decimal = Decimal("5000"),
) -> list[dict]:
    """
    Get a list of potential contract splitting candidates.

    Useful for manual review and reporting.

    Returns:
        List of dicts with vendor info and contract details
    """
    min_amount = threshold - buffer
    max_amount = threshold

    with get_session() as session:
        results = session.query(
            Vendor.id,
            Vendor.name,
            Vendor.vendor_id,
            func.count(Contract.id).label("count"),
            func.sum(Contract.current_value).label("total"),
        ).join(
            Contract, Contract.vendor_id == Vendor.id
        ).filter(
            Contract.current_value >= min_amount,
            Contract.current_value <= max_amount,
        ).group_by(
            Vendor.id
        ).having(
            func.count(Contract.id) >= min_contracts
        ).order_by(
            func.count(Contract.id).desc()
        ).all()

        return [
            {
                "vendor_id": r.id,
                "vendor_name": r.name,
                "vendor_vid": r.vendor_id,
                "contract_count": r.count,
                "total_value": float(r.total) if r.total else 0,
            }
            for r in results
        ]
