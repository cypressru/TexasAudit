"""
Confidentiality Analysis.

Detects unusual patterns in confidentiality-flagged transactions:
- Agencies with unusually high confidential transaction rates
- Sudden increases in confidentiality flagging
- Confidential transactions concentrated with specific vendors
"""

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, Integer, case, and_

from fraudit.database import get_session, Payment, Vendor, Agency
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run confidentiality analysis.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    rate_threshold = thresholds.get("confidentiality_rate_threshold", 0.20)

    print(f"  Checking confidentiality patterns (threshold: {rate_threshold:.0%})")

    with get_session() as session:
        # High overall confidentiality rates
        alerts_created += _detect_high_confidential_rates(session, rate_threshold)

        # Sudden increases
        alerts_created += _detect_confidentiality_spikes(session)

        # Vendor concentration
        alerts_created += _detect_vendor_confidentiality(session)

    return alerts_created


def _detect_high_confidential_rates(session, rate_threshold: float) -> int:
    """Find agencies with unusually high confidential transaction rates."""
    alerts_created = 0

    # Calculate confidentiality rate by agency
    results = session.query(
        Payment.agency_id,
        func.count(Payment.id).label("total_count"),
        func.sum(func.cast(Payment.is_confidential, Integer)).label("confidential_count"),
        func.sum(Payment.amount).label("total_amount"),
        func.sum(
            case(
                (Payment.is_confidential == True, Payment.amount),
                else_=Decimal("0")
            )
        ).label("confidential_amount"),
    ).filter(
        Payment.agency_id.isnot(None),
    ).group_by(Payment.agency_id).having(
        func.count(Payment.id) >= 100  # Minimum sample size
    ).all()

    for result in results:
        if not result.total_count:
            continue

        conf_rate = result.confidential_count / result.total_count
        conf_amount_rate = (
            float(result.confidential_amount) / float(result.total_amount)
            if result.total_amount else 0
        )

        if conf_rate < rate_threshold:
            continue

        agency = session.get(Agency, result.agency_id)
        if not agency:
            continue

        # Update agency's stored rate
        agency.confidentiality_rate = Decimal(str(conf_rate))

        evidence = {
            "agency_id": agency.id,
            "agency_name": agency.name,
            "total_transactions": result.total_count,
            "confidential_transactions": result.confidential_count,
            "confidentiality_rate": round(conf_rate * 100, 1),
            "total_amount": float(result.total_amount) if result.total_amount else 0,
            "confidential_amount": float(result.confidential_amount) if result.confidential_amount else 0,
            "confidential_amount_rate": round(conf_amount_rate * 100, 1),
        }

        severity = "medium"
        if conf_rate >= 0.40:
            severity = "high"

        alert_id = create_alert(
            alert_type="high_confidentiality_rate",
            severity=severity,
            title=f"High confidentiality rate: {agency.name}",
            description=(
                f"{agency.name} has {conf_rate:.1%} of transactions marked confidential "
                f"({result.confidential_count:,} of {result.total_count:,}). "
                f"Confidential amount: ${result.confidential_amount:,.2f} "
                f"({conf_amount_rate:.1%} of total). "
                f"This exceeds the {rate_threshold:.0%} threshold."
            ),
            entity_type="agency",
            entity_id=agency.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_confidentiality_spikes(session) -> int:
    """Detect sudden increases in confidential transactions."""
    alerts_created = 0

    # Compare recent period to historical
    recent_start = date.today() - timedelta(days=90)
    historical_start = date.today() - timedelta(days=365)

    # Get agency confidentiality rates for both periods
    agencies = session.query(Agency).all()

    for agency in agencies:
        # Recent period
        recent = session.query(
            func.count(Payment.id).label("total"),
            func.sum(func.cast(Payment.is_confidential, Integer)).label("confidential"),
        ).filter(
            Payment.agency_id == agency.id,
            Payment.payment_date >= recent_start,
        ).first()

        # Historical period (excluding recent)
        historical = session.query(
            func.count(Payment.id).label("total"),
            func.sum(func.cast(Payment.is_confidential, Integer)).label("confidential"),
        ).filter(
            Payment.agency_id == agency.id,
            Payment.payment_date >= historical_start,
            Payment.payment_date < recent_start,
        ).first()

        if not recent or not historical:
            continue
        if not recent.total or recent.total < 50:
            continue
        if not historical.total or historical.total < 100:
            continue

        recent_rate = (recent.confidential or 0) / recent.total
        historical_rate = (historical.confidential or 0) / historical.total

        # Significant increase
        if recent_rate > historical_rate + 0.10:  # 10 percentage point increase
            evidence = {
                "agency_id": agency.id,
                "agency_name": agency.name,
                "recent_rate": round(recent_rate * 100, 1),
                "historical_rate": round(historical_rate * 100, 1),
                "increase": round((recent_rate - historical_rate) * 100, 1),
                "recent_transactions": recent.total,
                "recent_confidential": recent.confidential or 0,
            }

            severity = "medium"
            if recent_rate - historical_rate >= 0.25:
                severity = "high"

            alert_id = create_alert(
                alert_type="confidentiality_spike",
                severity=severity,
                title=f"Confidentiality spike: {agency.name}",
                description=(
                    f"{agency.name} confidential transaction rate increased from "
                    f"{historical_rate:.1%} to {recent_rate:.1%} "
                    f"(+{(recent_rate - historical_rate):.1%}) in the last 90 days."
                ),
                entity_type="agency",
                entity_id=agency.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _detect_vendor_confidentiality(session) -> int:
    """Find vendors receiving high proportion of confidential payments."""
    alerts_created = 0

    # Find vendors where most payments are confidential
    results = session.query(
        Payment.vendor_id,
        func.count(Payment.id).label("total"),
        func.sum(func.cast(Payment.is_confidential, Integer)).label("confidential"),
        func.sum(Payment.amount).label("total_amount"),
    ).filter(
        Payment.vendor_id.isnot(None),
    ).group_by(Payment.vendor_id).having(
        func.count(Payment.id) >= 10  # Minimum transactions
    ).all()

    for result in results:
        if not result.total:
            continue

        conf_rate = (result.confidential or 0) / result.total

        # Flag if most payments are confidential
        if conf_rate < 0.50:
            continue

        vendor = session.get(Vendor, result.vendor_id)
        if not vendor:
            continue

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "total_payments": result.total,
            "confidential_payments": result.confidential or 0,
            "confidential_rate": round(conf_rate * 100, 1),
            "total_amount": float(result.total_amount) if result.total_amount else 0,
        }

        severity = "low"
        if conf_rate >= 0.75:
            severity = "medium"
        if conf_rate >= 0.90 and float(result.total_amount or 0) >= 100000:
            severity = "high"

        alert_id = create_alert(
            alert_type="vendor_high_confidentiality",
            severity=severity,
            title=f"High confidentiality: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has {conf_rate:.0%} confidential payments "
                f"({result.confidential or 0} of {result.total}). "
                f"Total value: ${result.total_amount:,.2f}."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def get_confidentiality_summary() -> dict:
    """Get overall confidentiality statistics."""
    with get_session() as session:
        total = session.query(func.count(Payment.id)).scalar() or 0
        confidential = session.query(func.count(Payment.id)).filter(
            Payment.is_confidential == True
        ).scalar() or 0

        total_amount = session.query(func.sum(Payment.amount)).scalar() or Decimal("0")
        conf_amount = session.query(func.sum(Payment.amount)).filter(
            Payment.is_confidential == True
        ).scalar() or Decimal("0")

        return {
            "total_transactions": total,
            "confidential_transactions": confidential,
            "confidential_rate": round(confidential / total * 100, 2) if total else 0,
            "total_amount": float(total_amount),
            "confidential_amount": float(conf_amount),
            "confidential_amount_rate": round(
                float(conf_amount) / float(total_amount) * 100, 2
            ) if total_amount else 0,
        }
