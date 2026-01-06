"""
Debarment detection module.

Checks vendors against federal (SAM.gov) and state exclusion lists
to identify potentially prohibited vendors receiving government payments.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional

from rapidfuzz import fuzz
from sqlalchemy import func
from tqdm import tqdm

from fraudit.database import (
    get_session,
    Vendor,
    Payment,
    DebarredEntity,
    Alert,
    AlertSeverity,
    AlertStatus,
)
from fraudit.normalization import normalize_vendor_name


def run_debarment_detection(
    name_threshold: float = 0.90,
    address_matching: bool = True,
    min_payment_amount: float = 1000,
) -> int:
    """
    Check all active vendors against exclusion lists.

    Args:
        name_threshold: Fuzzy match threshold for names (0-1)
        address_matching: Also check address matches
        min_payment_amount: Only check vendors with payments above this amount

    Returns:
        Number of alerts generated
    """
    print("Running debarment detection...")
    alerts_created = 0

    with get_session() as session:
        # Get active exclusions
        exclusions = session.query(DebarredEntity).filter(
            DebarredEntity.is_active == True
        ).all()

        if not exclusions:
            print("  No active exclusions in database. Run sam_exclusions sync first.")
            return 0

        print(f"  Checking against {len(exclusions):,} active exclusions...")

        # Build lookup structures for faster matching
        exclusion_names = {}  # normalized_name -> list of exclusions
        exclusion_addresses = {}  # normalized_address -> list of exclusions

        for exc in exclusions:
            # Name lookup
            if exc.name_normalized:
                if exc.name_normalized not in exclusion_names:
                    exclusion_names[exc.name_normalized] = []
                exclusion_names[exc.name_normalized].append(exc)

            # Address lookup (if enabled)
            if address_matching and exc.address:
                addr_key = normalize_vendor_name(exc.address[:100])
                if addr_key not in exclusion_addresses:
                    exclusion_addresses[addr_key] = []
                exclusion_addresses[addr_key].append(exc)

        # Get vendors with payments above threshold
        vendors_query = session.query(Vendor).join(Payment).group_by(Vendor.id).having(
            func.sum(Payment.amount) >= min_payment_amount
        )

        vendors = vendors_query.all()
        print(f"  Checking {len(vendors):,} vendors with payments >= ${min_payment_amount:,.0f}...")

        # Check each vendor
        for vendor in tqdm(vendors, desc="  Checking vendors"):
            matches = []

            # 1. Exact name match
            if vendor.name_normalized in exclusion_names:
                for exc in exclusion_names[vendor.name_normalized]:
                    matches.append({
                        "exclusion": exc,
                        "match_type": "exact_name",
                        "score": 1.0,
                    })

            # 2. Fuzzy name match (if no exact match)
            if not matches:
                for exc_name, exc_list in exclusion_names.items():
                    score = fuzz.ratio(vendor.name_normalized or "", exc_name) / 100.0
                    if score >= name_threshold:
                        for exc in exc_list:
                            matches.append({
                                "exclusion": exc,
                                "match_type": "fuzzy_name",
                                "score": score,
                            })

            # 3. Address match (if enabled and vendor has address)
            if address_matching and vendor.address:
                vendor_addr = normalize_vendor_name(vendor.address[:100])
                if vendor_addr in exclusion_addresses:
                    for exc in exclusion_addresses[vendor_addr]:
                        # Don't duplicate if already matched by name
                        already_matched = any(
                            m["exclusion"].id == exc.id for m in matches
                        )
                        if not already_matched:
                            matches.append({
                                "exclusion": exc,
                                "match_type": "address",
                                "score": 0.85,  # Lower confidence for address-only
                            })

            # Create alerts for matches
            for match in matches:
                exc = match["exclusion"]

                # Check if alert already exists
                existing = session.query(Alert).filter(
                    Alert.alert_type == "debarred_vendor",
                    Alert.entity_type == "vendor",
                    Alert.entity_id == vendor.id,
                ).first()

                if existing:
                    # Update evidence if new match is better
                    old_score = existing.evidence.get("match_score", 0) if existing.evidence else 0
                    if match["score"] > old_score:
                        existing.evidence = _build_evidence(vendor, match, session)
                    continue

                # Determine severity
                if match["match_type"] == "exact_name":
                    severity = AlertSeverity.HIGH
                elif match["score"] >= 0.95:
                    severity = AlertSeverity.HIGH
                elif match["score"] >= 0.90:
                    severity = AlertSeverity.MEDIUM
                else:
                    severity = AlertSeverity.LOW

                # Get vendor payment total
                payment_total = session.query(func.sum(Payment.amount)).filter(
                    Payment.vendor_id == vendor.id
                ).scalar() or 0

                alert = Alert(
                    alert_type="debarred_vendor",
                    severity=severity,
                    title=f"Vendor matches excluded entity: {exc.entity_name[:50]}",
                    description=(
                        f"Vendor '{vendor.name}' matches {match['match_type'].replace('_', ' ')} "
                        f"with excluded entity '{exc.entity_name}' "
                        f"(SAM: {exc.sam_number or 'N/A'}, {exc.source}). "
                        f"Total payments: ${float(payment_total):,.2f}"
                    ),
                    entity_type="vendor",
                    entity_id=vendor.id,
                    status=AlertStatus.NEW,
                    evidence=_build_evidence(vendor, match, session),
                )
                session.add(alert)
                alerts_created += 1

        session.commit()

    print(f"  Generated {alerts_created} debarment alerts")
    return alerts_created


def _build_evidence(vendor, match: dict, session) -> dict:
    """Build evidence dict for debarment alert."""
    exc = match["exclusion"]

    # Get payment details
    payments = session.query(Payment).filter(
        Payment.vendor_id == vendor.id
    ).order_by(Payment.amount.desc()).limit(10).all()

    payment_total = session.query(func.sum(Payment.amount)).filter(
        Payment.vendor_id == vendor.id
    ).scalar() or 0

    return {
        "match_type": match["match_type"],
        "match_score": match["score"],
        "vendor": {
            "id": vendor.id,
            "name": vendor.name,
            "name_normalized": vendor.name_normalized,
            "address": vendor.address,
            "city": vendor.city,
            "state": vendor.state,
            "vendor_id": vendor.vendor_id,
        },
        "exclusion": {
            "id": exc.id,
            "source": exc.source,
            "sam_number": exc.sam_number,
            "entity_name": exc.entity_name,
            "exclusion_type": exc.exclusion_type,
            "excluding_agency": exc.excluding_agency,
            "start_date": exc.start_date.isoformat() if exc.start_date else None,
            "end_date": exc.end_date.isoformat() if exc.end_date else None,
            "reason": exc.reason[:500] if exc.reason else None,
        },
        "payments": {
            "total_amount": float(payment_total),
            "count": len(payments),
            "largest_payments": [
                {
                    "id": p.id,
                    "amount": float(p.amount),
                    "date": p.payment_date.isoformat() if p.payment_date else None,
                    "agency": p.agency.name if p.agency else None,
                }
                for p in payments[:5]
            ],
        },
    }


def check_single_vendor(vendor_id: int, threshold: float = 0.85) -> list[dict]:
    """
    Check a single vendor against exclusion lists.

    Args:
        vendor_id: Vendor ID to check
        threshold: Match threshold

    Returns:
        List of matches with details
    """
    matches = []

    with get_session() as session:
        vendor = session.query(Vendor).filter(Vendor.id == vendor_id).first()
        if not vendor:
            return []

        exclusions = session.query(DebarredEntity).filter(
            DebarredEntity.is_active == True
        ).all()

        for exc in exclusions:
            # Exact match
            if vendor.name_normalized == exc.name_normalized:
                matches.append({
                    "exclusion_id": exc.id,
                    "entity_name": exc.entity_name,
                    "source": exc.source,
                    "sam_number": exc.sam_number,
                    "match_type": "exact",
                    "score": 1.0,
                    "exclusion_type": exc.exclusion_type,
                    "excluding_agency": exc.excluding_agency,
                })
                continue

            # Fuzzy match
            score = fuzz.ratio(vendor.name_normalized or "", exc.name_normalized or "") / 100.0
            if score >= threshold:
                matches.append({
                    "exclusion_id": exc.id,
                    "entity_name": exc.entity_name,
                    "source": exc.source,
                    "sam_number": exc.sam_number,
                    "match_type": "fuzzy",
                    "score": score,
                    "exclusion_type": exc.exclusion_type,
                    "excluding_agency": exc.excluding_agency,
                })

    return sorted(matches, key=lambda x: x["score"], reverse=True)


def get_debarment_stats() -> dict:
    """Get summary statistics about debarment data."""
    with get_session() as session:
        total = session.query(DebarredEntity).count()
        active = session.query(DebarredEntity).filter(
            DebarredEntity.is_active == True
        ).count()

        by_source = session.query(
            DebarredEntity.source,
            func.count(DebarredEntity.id)
        ).group_by(DebarredEntity.source).all()

        alerts = session.query(Alert).filter(
            Alert.alert_type == "debarred_vendor"
        ).count()

        return {
            "total_exclusions": total,
            "active_exclusions": active,
            "by_source": dict(by_source),
            "debarment_alerts": alerts,
        }


def detect(thresholds: dict) -> int:
    """
    Standard detection function interface for the detection engine.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    print("Checking vendors against exclusion lists...")

    # Get threshold from config or use default
    name_threshold = thresholds.get("debarment_name_similarity", 0.90)
    min_payment = thresholds.get("debarment_min_payment", 1000)

    return run_debarment_detection(
        name_threshold=name_threshold,
        address_matching=True,
        min_payment_amount=min_payment,
    )
