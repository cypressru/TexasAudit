"""
Payment Anomaly Detection.

Detects various suspicious payment patterns:
- Round number payments
- Large first payment to new vendor
- Payments exceeding contract value
- End of fiscal year payment spikes
- Ghost vendors (not in CMBL)
"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, and_, case

from texasaudit.database import get_session, Payment, Vendor, Contract, Agency
from texasaudit.normalization import to_state_fiscal_year
from texasaudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run payment anomaly detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    round_amounts = thresholds.get("round_number_amounts", [10000, 25000, 50000, 100000])
    large_first_payment = Decimal(str(thresholds.get("new_vendor_large_payment", 100000)))

    print("  Checking payment anomalies...")

    with get_session() as session:
        # Round number payments
        alerts_created += _detect_round_numbers(session, round_amounts)

        # Large first payments to new vendors
        alerts_created += _detect_large_first_payments(session, large_first_payment)

        # Payments exceeding contracts
        alerts_created += _detect_over_contract_payments(session)

        # Ghost vendors
        alerts_created += _detect_ghost_vendors(session)

        # Fiscal year end spikes
        alerts_created += _detect_fy_end_spikes(session)

    return alerts_created


def _detect_round_numbers(session, round_amounts: list) -> int:
    """Detect suspicious round number payments."""
    alerts_created = 0

    # Find vendors with multiple round-number payments
    for amount in round_amounts:
        amount = Decimal(str(amount))

        # Find vendors with many exact round payments
        results = session.query(
            Payment.vendor_id,
            func.count(Payment.id).label("count"),
            func.sum(Payment.amount).label("total"),
        ).filter(
            Payment.vendor_id.isnot(None),
            Payment.amount == amount,
        ).group_by(
            Payment.vendor_id,
        ).having(
            func.count(Payment.id) >= 5  # At least 5 round payments
        ).all()

        for result in results:
            vendor = session.get(Vendor, result.vendor_id)
            if not vendor:
                continue

            # Get total payment count to calculate percentage
            total_payments = session.query(func.count(Payment.id)).filter(
                Payment.vendor_id == result.vendor_id
            ).scalar()

            round_pct = (result.count / total_payments * 100) if total_payments else 0

            # Flag if high percentage are round numbers
            if round_pct >= 25:  # 25% or more are this exact round number
                evidence = {
                    "vendor_id": vendor.id,
                    "vendor_name": vendor.name,
                    "round_amount": float(amount),
                    "round_count": result.count,
                    "total_payments": total_payments,
                    "round_percentage": round(round_pct, 1),
                    "total_round_value": float(result.total),
                }

                severity = "low"
                if round_pct >= 50 or float(result.total) >= 500000:
                    severity = "medium"

                alert_id = create_alert(
                    alert_type="round_number_payments",
                    severity=severity,
                    title=f"Unusual round-number payments: {vendor.name}",
                    description=(
                        f"Vendor '{vendor.name}' has {result.count} payments of exactly "
                        f"${amount:,.2f} ({round_pct:.1f}% of all payments). "
                        f"Total value: ${result.total:,.2f}. "
                        f"Round-number payments may indicate estimation rather than actual invoicing."
                    ),
                    entity_type="vendor",
                    entity_id=vendor.id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created


def _detect_large_first_payments(session, threshold: Decimal) -> int:
    """Detect new vendors receiving large first payments."""
    alerts_created = 0

    # Find vendors whose first payment was very large
    # Subquery to get first payment date per vendor
    first_payment_sq = session.query(
        Payment.vendor_id,
        func.min(Payment.payment_date).label("first_date"),
    ).filter(
        Payment.vendor_id.isnot(None),
        Payment.payment_date.isnot(None),
    ).group_by(Payment.vendor_id).subquery()

    # Find first payments that exceed threshold
    results = session.query(Payment).join(
        first_payment_sq,
        and_(
            Payment.vendor_id == first_payment_sq.c.vendor_id,
            Payment.payment_date == first_payment_sq.c.first_date,
        )
    ).filter(
        Payment.amount >= threshold,
    ).all()

    # Check if these are truly new vendors (added within last 2 years)
    cutoff = date.today() - timedelta(days=730)

    for payment in results:
        vendor = session.get(Vendor, payment.vendor_id)
        if not vendor:
            continue

        # Skip if vendor has been around a long time
        if vendor.first_seen and vendor.first_seen < cutoff:
            continue

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "first_payment_amount": float(payment.amount),
            "first_payment_date": payment.payment_date.isoformat() if payment.payment_date else None,
            "vendor_first_seen": vendor.first_seen.isoformat() if vendor.first_seen else None,
            "in_cmbl": vendor.in_cmbl,
            "agency": payment.agency.name if payment.agency else None,
        }

        severity = "medium"
        if float(payment.amount) >= 500000:
            severity = "high"

        alert_id = create_alert(
            alert_type="large_first_payment",
            severity=severity,
            title=f"Large first payment to new vendor: {vendor.name}",
            description=(
                f"New vendor '{vendor.name}' received ${payment.amount:,.2f} as their "
                f"first payment on {payment.payment_date}. "
                f"Large initial payments to new vendors may warrant additional scrutiny."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_over_contract_payments(session) -> int:
    """Detect payments that exceed contract maximum values."""
    alerts_created = 0

    # For each contract, sum payments and compare to max value
    contracts = session.query(Contract).filter(
        Contract.max_value.isnot(None),
        Contract.max_value > 0,
        Contract.vendor_id.isnot(None),
    ).all()

    for contract in contracts:
        # Sum all payments to this vendor from this agency
        total_payments = session.query(func.sum(Payment.amount)).filter(
            Payment.vendor_id == contract.vendor_id,
            Payment.agency_id == contract.agency_id,
        ).scalar() or Decimal("0")

        if total_payments > contract.max_value:
            vendor = session.get(Vendor, contract.vendor_id)
            agency = session.get(Agency, contract.agency_id) if contract.agency_id else None

            excess = total_payments - contract.max_value
            excess_pct = (excess / contract.max_value * 100) if contract.max_value else 0

            evidence = {
                "contract_number": contract.contract_number,
                "contract_max_value": float(contract.max_value),
                "total_payments": float(total_payments),
                "excess_amount": float(excess),
                "excess_percentage": round(float(excess_pct), 1),
                "vendor_id": vendor.id if vendor else None,
                "vendor_name": vendor.name if vendor else None,
                "agency_name": agency.name if agency else None,
            }

            severity = "medium"
            if excess_pct >= 50 or float(excess) >= 100000:
                severity = "high"

            alert_id = create_alert(
                alert_type="over_contract_payment",
                severity=severity,
                title=f"Payments exceed contract: {contract.contract_number}",
                description=(
                    f"Contract {contract.contract_number} has max value ${contract.max_value:,.2f} "
                    f"but total payments are ${total_payments:,.2f} "
                    f"(${excess:,.2f} / {excess_pct:.1f}% over). "
                    f"Vendor: {vendor.name if vendor else 'Unknown'}."
                ),
                entity_type="contract",
                entity_id=contract.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _detect_ghost_vendors(session) -> int:
    """Detect vendors receiving payments but not in CMBL."""
    alerts_created = 0

    # Find vendors with significant payments who are not in CMBL
    results = session.query(
        Vendor.id,
        Vendor.name,
        func.sum(Payment.amount).label("total"),
        func.count(Payment.id).label("count"),
    ).join(Payment, Payment.vendor_id == Vendor.id).filter(
        Vendor.in_cmbl == False,
    ).group_by(Vendor.id).having(
        func.sum(Payment.amount) >= 50000  # Significant amount
    ).all()

    for result in results:
        vendor = session.get(Vendor, result.id)

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "total_payments": float(result.total),
            "payment_count": result.count,
            "address": vendor.address,
            "in_cmbl": False,
        }

        severity = "medium"
        if float(result.total) >= 500000:
            severity = "high"

        alert_id = create_alert(
            alert_type="ghost_vendor",
            severity=severity,
            title=f"Payments to non-CMBL vendor: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has received ${result.total:,.2f} "
                f"across {result.count} payments but is not registered in the "
                f"Centralized Master Bidders List (CMBL). "
                f"This may indicate an unregistered vendor or data quality issue."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_fy_end_spikes(session) -> int:
    """Detect unusual spending spikes at fiscal year end."""
    alerts_created = 0

    current_fy = to_state_fiscal_year(date.today())

    for fy in range(current_fy - 5, current_fy):
        # Compare August spending (end of TX FY) to average monthly spending
        fy_start = date(fy - 1, 9, 1)
        fy_end = date(fy, 8, 31)
        august_start = date(fy, 8, 1)

        # Get agency-level spending patterns
        results = session.query(
            Payment.agency_id,
            func.sum(
                case(
                    (Payment.payment_date >= august_start, Payment.amount),
                    else_=Decimal("0")
                )
            ).label("august_total"),
            func.sum(Payment.amount).label("fy_total"),
            func.count(Payment.id).label("fy_count"),
        ).filter(
            Payment.payment_date >= fy_start,
            Payment.payment_date <= fy_end,
            Payment.agency_id.isnot(None),
        ).group_by(Payment.agency_id).all()

        for result in results:
            if not result.fy_total or result.fy_total == 0:
                continue

            august_pct = (result.august_total / result.fy_total * 100) if result.fy_total else 0

            # August should be ~8.33% (1/12) of annual spending
            # Flag if significantly higher
            if august_pct >= 20:  # More than 20% in last month
                agency = session.get(Agency, result.agency_id)
                if not agency:
                    continue

                evidence = {
                    "fiscal_year": fy,
                    "agency_id": agency.id,
                    "agency_name": agency.name,
                    "august_spending": float(result.august_total),
                    "fy_total_spending": float(result.fy_total),
                    "august_percentage": round(float(august_pct), 1),
                }

                severity = "low"
                if august_pct >= 35:
                    severity = "medium"

                alert_id = create_alert(
                    alert_type="fy_end_spike",
                    severity=severity,
                    title=f"FY{fy} year-end spending spike: {agency.name}",
                    description=(
                        f"{agency.name} spent ${result.august_total:,.2f} in August {fy} "
                        f"({august_pct:.1f}% of FY total). "
                        f"High year-end spending may indicate 'use it or lose it' behavior."
                    ),
                    entity_type="agency",
                    entity_id=agency.id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created
