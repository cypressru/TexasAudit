"""
Duplicate Payment Detection.

Identifies potentially duplicate payments:
- Same vendor, same amount, same date
- Same vendor, same amount, within X days
- Same amount to different vendors at same address
"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func

from texasaudit.database import get_session, Payment, Vendor, Agency
from texasaudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run duplicate payment detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    window_days = thresholds.get("duplicate_payment_window_days", 30)

    print(f"  Checking for duplicate payments within {window_days} day window")

    with get_session() as session:
        # Exact duplicates (same vendor, amount, date)
        alerts_created += _find_exact_duplicates(session)

        # Near duplicates (same vendor, amount, within window)
        alerts_created += _find_near_duplicates(session, window_days)

        # Same amount to related vendors
        alerts_created += _find_related_vendor_duplicates(session, window_days)

    return alerts_created


def _find_exact_duplicates(session) -> int:
    """Find exact duplicate payments."""
    alerts_created = 0

    # Find payments with exact matches
    duplicates = session.query(
        Payment.vendor_id,
        Payment.amount,
        Payment.payment_date,
        func.count(Payment.id).label("count"),
        func.array_agg(Payment.id).label("payment_ids"),
    ).filter(
        Payment.vendor_id.isnot(None),
        Payment.amount > 100,  # Ignore tiny amounts
    ).group_by(
        Payment.vendor_id,
        Payment.amount,
        Payment.payment_date,
    ).having(
        func.count(Payment.id) > 1
    ).all()

    for dup in duplicates:
        vendor = session.get(Vendor, dup.vendor_id)
        if not vendor:
            continue

        # Get the actual payments
        payments = session.query(Payment).filter(
            Payment.id.in_(dup.payment_ids)
        ).all()

        # Get agency names
        agencies = set()
        for p in payments:
            if p.agency:
                agencies.add(p.agency.name)

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "amount": float(dup.amount),
            "payment_date": dup.payment_date.isoformat() if dup.payment_date else None,
            "duplicate_count": dup.count,
            "total_duplicate_amount": float(dup.amount * dup.count),
            "agencies": list(agencies),
            "payments": [
                {
                    "id": p.id,
                    "agency": p.agency.name if p.agency else None,
                    "description": p.description[:100] if p.description else None,
                    "source": p.source_system,
                }
                for p in payments
            ],
        }

        # Severity based on amount and count
        severity = "low"
        if dup.count >= 3 or float(dup.amount) >= 10000:
            severity = "medium"
        if dup.count >= 5 or float(dup.amount) >= 50000:
            severity = "high"

        alert_id = create_alert(
            alert_type="duplicate_payment",
            severity=severity,
            title=f"Exact duplicate payments: {vendor.name}",
            description=(
                f"Found {dup.count} identical payments of ${dup.amount:,.2f} "
                f"to '{vendor.name}' on {dup.payment_date}. "
                f"Total duplicate amount: ${float(dup.amount * dup.count):,.2f}."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _find_near_duplicates(session, window_days: int) -> int:
    """Find near-duplicate payments (same amount within window)."""
    alerts_created = 0

    # This is more complex - need to find payments that are similar but not exact
    # Focus on larger amounts to avoid noise
    min_amount = Decimal("5000")

    # Get all significant payments grouped by vendor and amount
    payments = session.query(Payment).filter(
        Payment.vendor_id.isnot(None),
        Payment.amount >= min_amount,
        Payment.payment_date.isnot(None),
    ).order_by(
        Payment.vendor_id,
        Payment.amount,
        Payment.payment_date,
    ).all()

    # Group by vendor and amount
    grouped = defaultdict(list)
    for p in payments:
        key = (p.vendor_id, p.amount)
        grouped[key].append(p)

    # Check each group for near-duplicates
    for (vendor_id, amount), payments_list in grouped.items():
        if len(payments_list) < 2:
            continue

        # Sort by date
        payments_list.sort(key=lambda p: p.payment_date)

        # Find clusters within window
        clusters = []
        current_cluster = [payments_list[0]]

        for i in range(1, len(payments_list)):
            prev_date = payments_list[i - 1].payment_date
            curr_date = payments_list[i].payment_date

            if (curr_date - prev_date).days <= window_days:
                current_cluster.append(payments_list[i])
            else:
                if len(current_cluster) > 1:
                    clusters.append(current_cluster)
                current_cluster = [payments_list[i]]

        if len(current_cluster) > 1:
            clusters.append(current_cluster)

        # Create alerts for suspicious clusters
        for cluster in clusters:
            # Skip if all on same date (handled by exact duplicates)
            dates = set(p.payment_date for p in cluster)
            if len(dates) == 1:
                continue

            vendor = session.get(Vendor, vendor_id)
            if not vendor:
                continue

            evidence = {
                "vendor_id": vendor.id,
                "vendor_name": vendor.name,
                "amount": float(amount),
                "payment_count": len(cluster),
                "date_range": f"{min(dates)} to {max(dates)}",
                "payments": [
                    {
                        "id": p.id,
                        "date": p.payment_date.isoformat(),
                        "agency": p.agency.name if p.agency else None,
                    }
                    for p in cluster
                ],
            }

            severity = "low"
            if len(cluster) >= 4:
                severity = "medium"
            if len(cluster) >= 6 or float(amount) >= 25000:
                severity = "high"

            alert_id = create_alert(
                alert_type="near_duplicate_payment",
                severity=severity,
                title=f"Potential duplicate payments: {vendor.name}",
                description=(
                    f"Found {len(cluster)} payments of ${amount:,.2f} "
                    f"to '{vendor.name}' within {window_days} days "
                    f"({min(dates)} to {max(dates)}). "
                    f"Total: ${float(amount * len(cluster)):,.2f}."
                ),
                entity_type="vendor",
                entity_id=vendor.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _find_related_vendor_duplicates(session, window_days: int) -> int:
    """Find same payments to potentially related vendors."""
    alerts_created = 0

    # This will be more useful after vendor clustering runs
    # For now, check vendors at same address
    from texasaudit.database import VendorRelationship

    # Get vendor pairs with same_address relationship
    relationships = session.query(VendorRelationship).filter(
        VendorRelationship.relationship_type == "same_address"
    ).all()

    for rel in relationships:
        # Find payments to both vendors with same amount
        vendor1_payments = session.query(Payment).filter(
            Payment.vendor_id == rel.vendor_id_1,
            Payment.amount >= 1000,
        ).all()

        vendor2_payments = session.query(Payment).filter(
            Payment.vendor_id == rel.vendor_id_2,
            Payment.amount >= 1000,
        ).all()

        # Check for matching amounts
        v1_amounts = {p.amount: p for p in vendor1_payments}
        for p2 in vendor2_payments:
            if p2.amount in v1_amounts:
                p1 = v1_amounts[p2.amount]

                # Check if within time window
                if p1.payment_date and p2.payment_date:
                    delta = abs((p1.payment_date - p2.payment_date).days)
                    if delta > window_days:
                        continue

                vendor1 = session.get(Vendor, rel.vendor_id_1)
                vendor2 = session.get(Vendor, rel.vendor_id_2)

                evidence = {
                    "vendor1_id": vendor1.id,
                    "vendor1_name": vendor1.name,
                    "vendor2_id": vendor2.id,
                    "vendor2_name": vendor2.name,
                    "amount": float(p2.amount),
                    "payment1_date": p1.payment_date.isoformat() if p1.payment_date else None,
                    "payment2_date": p2.payment_date.isoformat() if p2.payment_date else None,
                    "shared_address": vendor1.address,
                }

                alert_id = create_alert(
                    alert_type="related_vendor_duplicate",
                    severity="medium",
                    title=f"Same payment to related vendors",
                    description=(
                        f"Found ${p2.amount:,.2f} payment to both "
                        f"'{vendor1.name}' and '{vendor2.name}' "
                        f"(same address: {vendor1.address}). "
                        f"This may indicate duplicate payments or fraudulent vendors."
                    ),
                    entity_type="vendor",
                    entity_id=vendor1.id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created
