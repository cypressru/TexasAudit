"""
Employee-Vendor Match Detection (Optimized).

Detects potential conflicts of interest where:
- Employee names match vendor names (fuzzy matching)
- Employee addresses match vendor addresses
- Employees may be receiving payments through vendor entities

Uses multiprocessing for efficient CPU-bound fuzzy matching at scale.
"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from decimal import Decimal
from typing import Optional
import multiprocessing
import os

from rapidfuzz import fuzz, process
from sqlalchemy import func

from texasaudit.database import (
    get_session, Employee, Vendor, Payment, EntityMatch
)
from texasaudit.normalization import normalize_address
from texasaudit.alerts import create_alert


# Detect available CPU cores
CPU_COUNT = os.cpu_count() or 4


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

    print(f"  Employee-vendor matches (threshold: {name_similarity_threshold})")

    with get_session() as session:
        # Name-based matching (parallelized)
        alerts_created += _match_by_name_parallel(session, name_similarity_threshold)

        # Address-based matching
        alerts_created += _match_by_address(session)

    return alerts_created


def _process_employee_batch(args):
    """Worker function for multiprocessing - finds name matches for a batch of employees."""
    employee_data, vendor_names, threshold = args
    batch_matches = []

    for emp_id, emp_name, emp_name_norm in employee_data:
        if not emp_name_norm:
            continue

        # Find similar vendor names using rapidfuzz
        matches = process.extract(
            emp_name_norm,
            vendor_names,
            scorer=fuzz.ratio,
            score_cutoff=int(threshold * 100),
            limit=5,
        )

        for vendor_name, score, _ in matches:
            batch_matches.append({
                "emp_id": emp_id,
                "emp_name": emp_name,
                "vendor_name_norm": vendor_name,
                "confidence": score / 100.0,
            })

    return batch_matches


def _match_by_name_parallel(session, threshold: float, batch_size: int = 1000) -> int:
    """Find employees whose names match vendor names using multiprocessing."""
    alerts_created = 0

    # Load all data upfront
    employees = session.query(Employee).filter(
        Employee.name_normalized.isnot(None)
    ).all()

    vendors = session.query(Vendor).filter(
        Vendor.name_normalized.isnot(None)
    ).all()

    if not employees or not vendors:
        return 0

    # Use most CPU cores but leave 1-2 free for system
    num_workers = max(1, CPU_COUNT - 1)
    print(f"      Comparing {len(employees):,} employees × {len(vendors):,} vendors ({num_workers} cores)...")

    # Extract primitive data for multiprocessing (can't pickle SQLAlchemy objects)
    employee_data = [(e.id, e.name, e.name_normalized) for e in employees]
    vendor_names = [v.name_normalized for v in vendors]

    # Build vendor lookup by normalized name
    vendor_by_name = {v.name_normalized: v for v in vendors}

    # Pre-compute vendor payment stats (single query)
    vendor_payment_stats = {}
    payment_stats = session.query(
        Payment.vendor_id,
        func.count(Payment.id).label("count"),
        func.sum(Payment.amount).label("total"),
        func.max(Payment.payment_date).label("last_date"),
    ).filter(
        Payment.vendor_id.isnot(None)
    ).group_by(Payment.vendor_id).all()

    for stat in payment_stats:
        vendor_payment_stats[stat.vendor_id] = {
            "count": stat.count,
            "total": stat.total,
            "last_date": stat.last_date,
        }

    # Split employees into batches for parallel processing
    employee_batches = [
        employee_data[i:i + batch_size]
        for i in range(0, len(employee_data), batch_size)
    ]

    # Prepare args for worker processes
    work_args = [(batch, vendor_names, threshold) for batch in employee_batches]

    # Process batches in parallel using multiple CPU cores
    matches_found = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(_process_employee_batch, args) for args in work_args]

        for future in as_completed(futures):
            batch_matches = future.result()
            matches_found.extend(batch_matches)

    print(f"      Found {len(matches_found):,} potential matches, creating alerts...")

    # Build employee lookup for alert creation
    employee_by_id = {e.id: e for e in employees}

    # Now create alerts (must be done in main process for database safety)
    for match in matches_found:
        emp_id = match["emp_id"]
        vendor_name_norm = match["vendor_name_norm"]
        confidence = match["confidence"]

        employee = employee_by_id.get(emp_id)
        vendor = vendor_by_name.get(vendor_name_norm)

        if not employee or not vendor:
            continue

        # Get payment stats
        stats = vendor_payment_stats.get(vendor.id)
        if not stats or not stats["total"]:
            continue

        # Record the entity match
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
            "payment_count": stats["count"],
            "total_payments": float(stats["total"]),
            "last_payment_date": stats["last_date"].isoformat() if stats["last_date"] else None,
        }

        # Severity based on similarity and payment amount
        severity = "medium"
        if confidence >= 0.95 and float(stats["total"]) >= 100000:
            severity = "high"
        elif confidence >= 0.98:
            severity = "high"

        alert_id = create_alert(
            alert_type="employee_vendor_match",
            severity=severity,
            title=f"Employee-vendor name match: {employee.name}",
            description=(
                f"Employee '{employee.name}' ({employee.job_title or 'Unknown'} at "
                f"{employee.agency.name if employee.agency else 'Unknown'}) has "
                f"{confidence:.0%} name similarity with vendor '{vendor.name}'. "
                f"Vendor has received ${stats['total']:,.2f} in payments."
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

    # Extract and normalize addresses
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

    if not employee_addresses:
        return 0

    # Pre-compute vendor payment stats
    vendor_payment_stats = {}
    payment_stats = session.query(
        Payment.vendor_id,
        func.count(Payment.id).label("count"),
        func.sum(Payment.amount).label("total"),
    ).filter(
        Payment.vendor_id.isnot(None)
    ).group_by(Payment.vendor_id).all()

    for stat in payment_stats:
        vendor_payment_stats[stat.vendor_id] = {
            "count": stat.count,
            "total": stat.total,
        }

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

        if parsed.normalized not in employee_addresses:
            continue

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
            stats = vendor_payment_stats.get(vendor.id)
            if not stats or not stats["total"]:
                continue

            evidence = {
                "employee_id": employee.id,
                "employee_name": employee.name,
                "employee_agency": employee.agency.name if employee.agency else None,
                "vendor_id": vendor.id,
                "vendor_name": vendor.name,
                "shared_address": parsed.normalized,
                "payment_count": stats["count"],
                "total_payments": float(stats["total"]),
            }

            alert_id = create_alert(
                alert_type="employee_vendor_address_match",
                severity="high",
                title=f"Employee-vendor address match: {employee.name}",
                description=(
                    f"Employee '{employee.name}' at {employee.agency.name if employee.agency else 'Unknown'} "
                    f"shares address with vendor '{vendor.name}' ({parsed.normalized}). "
                    f"Vendor has received ${stats['total']:,.2f} in {stats['count']} payments."
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
    existing = session.query(EntityMatch).filter(
        EntityMatch.entity_type_1 == entity_type_1,
        EntityMatch.entity_id_1 == entity_id_1,
        EntityMatch.entity_type_2 == entity_type_2,
        EntityMatch.entity_id_2 == entity_id_2,
        EntityMatch.match_type == match_type,
    ).first()

    if existing:
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
