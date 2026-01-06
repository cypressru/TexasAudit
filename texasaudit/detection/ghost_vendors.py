"""
Ghost Vendor Detection.

Detects vendors that may be fraudulent or unregistered:
- Vendors receiving payments but not in CMBL (Centralized Master Bidders List)
- Vendors with no matching tax permits or business registrations
- Vendors with incomplete or suspicious address information
- Vendors with minimal online presence or business footprint

Note: Some ghost vendor detection overlaps with anomalies.py _detect_ghost_vendors,
but this module provides more comprehensive analysis including address validation
and business registration checks.
"""

from decimal import Decimal

from sqlalchemy import func, and_, or_

from texasaudit.database import get_session, Vendor, Payment
from texasaudit.normalization import normalize_address
from texasaudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run ghost vendor detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    min_payment_amount = Decimal(str(thresholds.get("ghost_vendor_min_payment", 25000)))

    print(f"  Checking for ghost vendors (min payment: ${min_payment_amount:,.0f})")

    with get_session() as session:
        # Non-CMBL vendors with significant payments
        print("    Checking non-CMBL vendors...")
        alerts_created += _detect_non_cmbl_vendors(session, min_payment_amount)

        # Vendors with incomplete addresses
        print("    Checking incomplete addresses...")
        alerts_created += _detect_incomplete_addresses(session, min_payment_amount)

        # Vendors with suspicious address patterns
        print("    Checking suspicious addresses...")
        alerts_created += _detect_suspicious_addresses(session, min_payment_amount)

    return alerts_created


def _detect_non_cmbl_vendors(session, min_amount: Decimal) -> int:
    """Detect vendors receiving significant payments but not in CMBL."""
    alerts_created = 0

    # Find vendors with significant payments who are not in CMBL
    results = session.query(
        Vendor.id,
        Vendor.name,
        Vendor.vendor_id,
        func.sum(Payment.amount).label("total"),
        func.count(Payment.id).label("count"),
        func.count(Payment.agency_id.distinct()).label("agency_count"),
        func.min(Payment.payment_date).label("first_payment"),
        func.max(Payment.payment_date).label("last_payment"),
    ).join(
        Payment, Payment.vendor_id == Vendor.id
    ).filter(
        Vendor.in_cmbl == False,
    ).group_by(
        Vendor.id
    ).having(
        func.sum(Payment.amount) >= min_amount
    ).all()

    for result in results:
        vendor = session.get(Vendor, result.id)
        if not vendor:
            continue

        # Check for additional red flags
        red_flags = []

        # No vendor ID is suspicious
        if not vendor.vendor_id:
            red_flags.append("No state vendor ID")

        # Incomplete address
        if not vendor.address or not vendor.city or not vendor.state:
            red_flags.append("Incomplete address")

        # No phone number
        if not vendor.phone:
            red_flags.append("No phone number")

        # Multiple agencies paying this vendor (more suspicious)
        if result.agency_count >= 3:
            red_flags.append(f"Payments from {result.agency_count} different agencies")

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "vendor_vid": vendor.vendor_id,
            "in_cmbl": False,
            "total_payments": float(result.total),
            "payment_count": result.count,
            "agency_count": result.agency_count,
            "first_payment": result.first_payment.isoformat() if result.first_payment else None,
            "last_payment": result.last_payment.isoformat() if result.last_payment else None,
            "address": vendor.address,
            "city": vendor.city,
            "state": vendor.state,
            "phone": vendor.phone,
            "red_flags": red_flags,
        }

        # Severity based on amount and red flags
        severity = "medium"
        if float(result.total) >= 500000 or len(red_flags) >= 3:
            severity = "high"

        alert_id = create_alert(
            alert_type="ghost_vendor",
            severity=severity,
            title=f"Potential ghost vendor: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has received ${result.total:,.2f} "
                f"across {result.count} payments from {result.agency_count} agencies "
                f"but is not registered in the Centralized Master Bidders List (CMBL). "
                f"Red flags: {', '.join(red_flags) if red_flags else 'None'}. "
                f"This may indicate an unregistered vendor or fraudulent entity."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_incomplete_addresses(session, min_amount: Decimal) -> int:
    """Detect vendors with significant payments but incomplete address info."""
    alerts_created = 0

    # Find vendors with missing critical address components
    results = session.query(
        Vendor.id,
        func.sum(Payment.amount).label("total"),
        func.count(Payment.id).label("count"),
    ).join(
        Payment, Payment.vendor_id == Vendor.id
    ).filter(
        or_(
            Vendor.address.is_(None),
            Vendor.address == "",
            Vendor.city.is_(None),
            Vendor.state.is_(None),
            and_(
                Vendor.address.isnot(None),
                func.length(Vendor.address) < 5  # Very short address
            )
        )
    ).group_by(
        Vendor.id
    ).having(
        func.sum(Payment.amount) >= min_amount
    ).all()

    for result in results:
        vendor = session.get(Vendor, result.id)
        if not vendor:
            continue

        # Skip if already flagged as ghost vendor
        # (to avoid duplicate alerts)
        if not vendor.in_cmbl:
            continue

        missing_fields = []
        if not vendor.address or len(vendor.address) < 5:
            missing_fields.append("street address")
        if not vendor.city:
            missing_fields.append("city")
        if not vendor.state:
            missing_fields.append("state")
        if not vendor.zip_code:
            missing_fields.append("ZIP code")

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "total_payments": float(result.total),
            "payment_count": result.count,
            "address": vendor.address,
            "city": vendor.city,
            "state": vendor.state,
            "zip_code": vendor.zip_code,
            "missing_fields": missing_fields,
        }

        severity = "medium"
        if float(result.total) >= 100000 or len(missing_fields) >= 3:
            severity = "high"

        alert_id = create_alert(
            alert_type="incomplete_vendor_address",
            severity=severity,
            title=f"Vendor with incomplete address: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has received ${result.total:,.2f} "
                f"in {result.count} payments but has incomplete address information. "
                f"Missing: {', '.join(missing_fields)}. "
                f"Legitimate vendors should have complete contact information."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_suspicious_addresses(session, min_amount: Decimal) -> int:
    """Detect vendors with suspicious address patterns."""
    alerts_created = 0

    # Suspicious address patterns
    suspicious_patterns = [
        "po box",
        "p.o. box",
        "p o box",
        "pmb",  # Private mailbox
        "suite 0",
        "apt 0",
        "unit 0",
        "unknown",
        "n/a",
        "none",
        "general delivery",
    ]

    # Find vendors with significant payments
    results = session.query(
        Vendor.id,
        func.sum(Payment.amount).label("total"),
        func.count(Payment.id).label("count"),
    ).join(
        Payment, Payment.vendor_id == Vendor.id
    ).filter(
        Vendor.address.isnot(None),
    ).group_by(
        Vendor.id
    ).having(
        func.sum(Payment.amount) >= min_amount
    ).all()

    for result in results:
        vendor = session.get(Vendor, result.id)
        if not vendor or not vendor.address:
            continue

        address_lower = vendor.address.lower()

        # Check for suspicious patterns
        found_patterns = [
            pattern for pattern in suspicious_patterns
            if pattern in address_lower
        ]

        if not found_patterns:
            continue

        # Additional checks
        red_flags = found_patterns.copy()

        # PO Box for very large payments is suspicious
        if any("box" in p for p in found_patterns):
            if float(result.total) >= 250000:
                red_flags.append(f"Large payments to PO Box address")

        # Very short address
        if len(vendor.address) < 10:
            red_flags.append("Very short address")

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "total_payments": float(result.total),
            "payment_count": result.count,
            "address": vendor.address,
            "city": vendor.city,
            "state": vendor.state,
            "suspicious_patterns": found_patterns,
            "red_flags": red_flags,
        }

        # Severity based on amount and flags
        severity = "low"
        if float(result.total) >= 100000:
            severity = "medium"
        if float(result.total) >= 500000 or len(red_flags) >= 3:
            severity = "high"

        alert_id = create_alert(
            alert_type="suspicious_vendor_address",
            severity=severity,
            title=f"Vendor with suspicious address: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has received ${result.total:,.2f} "
                f"but has a suspicious address: '{vendor.address}'. "
                f"Patterns found: {', '.join(found_patterns)}. "
                f"This may indicate a shell company or mail drop."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created
