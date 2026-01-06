"""
Employee-Vendor Match Detection.

Detects potential conflicts of interest where:
- Employee names match vendor names (fuzzy matching)
- Employee addresses match vendor addresses
- Employees may be receiving payments through vendor entities

This is a critical fraud indicator as it may reveal:
- Self-dealing
- Kickback schemes
- Unauthorized moonlighting
- Family/related party transactions
"""

from decimal import Decimal

from rapidfuzz import fuzz
from sqlalchemy import func

from texasaudit.database import (
    get_session, Employee, Vendor, Payment, EntityMatch
)
from texasaudit.normalization import normalize_address
from texasaudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run employee-vendor matching detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    name_similarity_threshold = thresholds.get("employee_vendor_name_similarity", 0.90)

    print(f"  Checking employee-vendor matches (name threshold: {name_similarity_threshold})")

    with get_session() as session:
        # Name-based matching
        print("    Matching by name...")
        alerts_created += _match_by_name(session, name_similarity_threshold)

        # Address-based matching
        print("    Matching by address...")
        alerts_created += _match_by_address(session)

    return alerts_created


def _match_by_name(session, threshold: float) -> int:
    """Find employees whose names match vendor names."""
    alerts_created = 0

    # Get all employees and vendors with normalized names
    employees = session.query(Employee).filter(
        Employee.name_normalized.isnot(None)
    ).all()

    vendors = session.query(Vendor).filter(
        Vendor.name_normalized.isnot(None)
    ).all()

    print(f"      Comparing {len(employees)} employees against {len(vendors)} vendors...")

    # For efficiency, build vendor name index
    vendor_by_name = {v.name_normalized: v for v in vendors}
    vendor_names = list(vendor_by_name.keys())

    # Match each employee
    for employee in employees:
        emp_name = employee.name_normalized
        if not emp_name:
            continue

        # Find similar vendor names using fuzzy matching
        from rapidfuzz import process

        matches = process.extract(
            emp_name,
            vendor_names,
            scorer=fuzz.ratio,
            score_cutoff=int(threshold * 100),
            limit=5,
        )

        for vendor_name, score, _ in matches:
            confidence = score / 100.0
            vendor = vendor_by_name[vendor_name]

            # Record the match
            _record_entity_match(
                session,
                entity_type_1="employee",
                entity_id_1=employee.id,
                entity_type_2="vendor",
                entity_id_2=vendor.id,
                match_type="name",
                confidence=confidence,
                evidence={
                    "employee_name": employee.name,
                    "vendor_name": vendor.name,
                    "similarity": confidence,
                }
            )

            # Get payment statistics for this vendor
            payment_stats = session.query(
                func.count(Payment.id).label("count"),
                func.sum(Payment.amount).label("total"),
                func.max(Payment.payment_date).label("last_date"),
            ).filter(
                Payment.vendor_id == vendor.id
            ).first()

            if not payment_stats or not payment_stats.total:
                continue

            # Create alert
            evidence = {
                "employee_id": employee.id,
                "employee_name": employee.name,
                "employee_agency": employee.agency.name if employee.agency else None,
                "employee_title": employee.job_title,
                "employee_salary": float(employee.annual_salary) if employee.annual_salary else None,
                "vendor_id": vendor.id,
                "vendor_name": vendor.name,
                "vendor_address": vendor.address,
                "name_similarity": confidence,
                "payment_count": payment_stats.count,
                "total_payments": float(payment_stats.total),
                "last_payment_date": payment_stats.last_date.isoformat() if payment_stats.last_date else None,
            }

            # Severity based on similarity and payment amount
            severity = "medium"
            if confidence >= 0.95 and float(payment_stats.total) >= 100000:
                severity = "high"
            elif confidence >= 0.98:  # Exact or near-exact match
                severity = "high"

            alert_id = create_alert(
                alert_type="employee_vendor_match",
                severity=severity,
                title=f"Employee-vendor name match: {employee.name}",
                description=(
                    f"Employee '{employee.name}' ({employee.job_title or 'Unknown'} at "
                    f"{employee.agency.name if employee.agency else 'Unknown'}) has "
                    f"{confidence:.0%} name similarity with vendor '{vendor.name}'. "
                    f"Vendor has received ${payment_stats.total:,.2f} in payments. "
                    f"This may indicate a conflict of interest or self-dealing."
                ),
                entity_type="employee",
                entity_id=employee.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _match_by_address(session) -> int:
    """Find employees and vendors sharing the same address."""
    alerts_created = 0

    # Get employees with addresses
    employees = session.query(Employee).filter(
        Employee.raw_data.isnot(None)
    ).all()

    # Extract addresses from raw_data (structure depends on your data source)
    employee_addresses = {}
    for emp in employees:
        if emp.raw_data and isinstance(emp.raw_data, dict):
            address = emp.raw_data.get('address')
            city = emp.raw_data.get('city')
            state = emp.raw_data.get('state')
            zip_code = emp.raw_data.get('zip')

            if address:
                parsed = normalize_address(address, city, state, zip_code)
                if parsed.normalized:
                    if parsed.normalized not in employee_addresses:
                        employee_addresses[parsed.normalized] = []
                    employee_addresses[parsed.normalized].append(emp)

    # Get vendors with addresses
    vendors = session.query(Vendor).filter(
        Vendor.address.isnot(None),
        Vendor.address != "",
    ).all()

    # Match vendors to employee addresses
    for vendor in vendors:
        parsed = normalize_address(
            vendor.address,
            vendor.city,
            vendor.state,
            vendor.zip_code
        )

        if not parsed.normalized:
            continue

        if parsed.normalized in employee_addresses:
            matching_employees = employee_addresses[parsed.normalized]

            for employee in matching_employees:
                # Record the match
                _record_entity_match(
                    session,
                    entity_type_1="employee",
                    entity_id_1=employee.id,
                    entity_type_2="vendor",
                    entity_id_2=vendor.id,
                    match_type="address",
                    confidence=0.85,
                    evidence={
                        "address": parsed.normalized,
                        "employee_name": employee.name,
                        "vendor_name": vendor.name,
                    }
                )

                # Get payment statistics
                payment_stats = session.query(
                    func.count(Payment.id).label("count"),
                    func.sum(Payment.amount).label("total"),
                ).filter(
                    Payment.vendor_id == vendor.id
                ).first()

                if not payment_stats or not payment_stats.total:
                    continue

                evidence = {
                    "employee_id": employee.id,
                    "employee_name": employee.name,
                    "employee_agency": employee.agency.name if employee.agency else None,
                    "vendor_id": vendor.id,
                    "vendor_name": vendor.name,
                    "shared_address": parsed.normalized,
                    "payment_count": payment_stats.count,
                    "total_payments": float(payment_stats.total),
                }

                # Address matches are generally more suspicious
                severity = "high"

                alert_id = create_alert(
                    alert_type="employee_vendor_address_match",
                    severity=severity,
                    title=f"Employee-vendor address match: {employee.name}",
                    description=(
                        f"Employee '{employee.name}' at {employee.agency.name if employee.agency else 'Unknown'} "
                        f"shares address with vendor '{vendor.name}' ({parsed.normalized}). "
                        f"Vendor has received ${payment_stats.total:,.2f} in {payment_stats.count} payments. "
                        f"This is a strong indicator of potential conflict of interest."
                    ),
                    entity_type="employee",
                    entity_id=employee.id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created


def _record_entity_match(
    session,
    entity_type_1: str,
    entity_id_1: int,
    entity_type_2: str,
    entity_id_2: int,
    match_type: str,
    confidence: float,
    evidence: dict,
) -> None:
    """Record an entity match in the database."""
    # Check if match already exists
    existing = session.query(EntityMatch).filter(
        EntityMatch.entity_type_1 == entity_type_1,
        EntityMatch.entity_id_1 == entity_id_1,
        EntityMatch.entity_type_2 == entity_type_2,
        EntityMatch.entity_id_2 == entity_id_2,
        EntityMatch.match_type == match_type,
    ).first()

    if existing:
        # Update if confidence is higher
        if confidence > float(existing.confidence_score or 0):
            existing.confidence_score = Decimal(str(confidence))
            existing.evidence = evidence
    else:
        match = EntityMatch(
            entity_type_1=entity_type_1,
            entity_id_1=entity_id_1,
            entity_type_2=entity_type_2,
            entity_id_2=entity_id_2,
            match_type=match_type,
            confidence_score=Decimal(str(confidence)),
            evidence=evidence,
            is_confirmed=False,
        )
        session.add(match)
