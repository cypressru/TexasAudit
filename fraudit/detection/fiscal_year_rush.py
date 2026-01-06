"""
Fiscal Year End Spending Rush Detection.

Detects suspicious end-of-year spending patterns:
- Agencies with unusual August/September spending spikes (Texas FY ends Aug 31)
- Spending that's significantly higher than monthly average
- Pattern of spending rushing to "use it or lose it" budget allocations
- Vendors who primarily receive payments at fiscal year end

This can indicate:
- Wasteful spending to avoid budget cuts
- Rushed procurement without proper oversight
- Coordination with vendors to spend remaining budget
- Financial manipulation to meet budget targets
"""

from collections import defaultdict
from datetime import date
from decimal import Decimal

from sqlalchemy import func, case, and_

from fraudit.database import get_session, Payment, Vendor, Agency
from fraudit.normalization import to_state_fiscal_year
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run fiscal year end spending detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    spike_multiplier = thresholds.get("fy_end_spike_multiplier", 2.0)
    min_amount = Decimal(str(thresholds.get("fy_end_min_amount", 100000)))

    print(f"  Checking fiscal year end spending (spike threshold: {spike_multiplier}x)")

    with get_session() as session:
        # Agency-level FY end spikes
        print("    Checking agency spending patterns...")
        alerts_created += _detect_agency_fy_spikes(
            session, spike_multiplier, min_amount
        )

        # Vendor-level FY end concentration
        print("    Checking vendor FY end concentration...")
        alerts_created += _detect_vendor_fy_concentration(session)

        # Unusual August/September payment clusters
        print("    Checking end-of-year payment clusters...")
        alerts_created += _detect_fy_end_clusters(session, min_amount)

    return alerts_created


def _detect_agency_fy_spikes(
    session,
    spike_multiplier: float,
    min_amount: Decimal
) -> int:
    """Detect agencies with unusual fiscal year-end spending spikes."""
    alerts_created = 0

    current_fy = to_state_fiscal_year(date.today())

    # Analyze last 5 fiscal years
    for fy in range(current_fy - 5, current_fy):
        # Texas FY: Sep 1 (fy-1) to Aug 31 (fy)
        fy_start = date(fy - 1, 9, 1)
        fy_end = date(fy, 8, 31)

        # Final two months: July and August
        final_months_start = date(fy, 7, 1)

        # Query agency spending patterns
        results = session.query(
            Payment.agency_id,
            # Total for fiscal year
            func.sum(Payment.amount).label("fy_total"),
            # Total for final two months
            func.sum(
                case(
                    (Payment.payment_date >= final_months_start, Payment.amount),
                    else_=Decimal("0")
                )
            ).label("final_months_total"),
            # July spending
            func.sum(
                case(
                    (
                        and_(
                            Payment.payment_date >= date(fy, 7, 1),
                            Payment.payment_date < date(fy, 8, 1)
                        ),
                        Payment.amount
                    ),
                    else_=Decimal("0")
                )
            ).label("july_total"),
            # August spending
            func.sum(
                case(
                    (
                        and_(
                            Payment.payment_date >= date(fy, 8, 1),
                            Payment.payment_date <= date(fy, 8, 31)
                        ),
                        Payment.amount
                    ),
                    else_=Decimal("0")
                )
            ).label("august_total"),
            func.count(Payment.id).label("payment_count"),
        ).filter(
            Payment.payment_date >= fy_start,
            Payment.payment_date <= fy_end,
            Payment.agency_id.isnot(None),
        ).group_by(
            Payment.agency_id
        ).all()

        for result in results:
            if not result.fy_total or result.fy_total == 0:
                continue

            # Calculate monthly average (excluding final 2 months)
            first_10_months_total = result.fy_total - result.final_months_total
            monthly_avg = first_10_months_total / 10 if first_10_months_total > 0 else Decimal("0")

            if monthly_avg == 0:
                continue

            # Check if final months are significantly higher than average
            final_months_ratio = float(result.final_months_total / monthly_avg) if monthly_avg else 0
            august_ratio = float(result.august_total / monthly_avg) if monthly_avg else 0

            # Flag if final months or August alone exceeds threshold
            is_suspicious = (
                final_months_ratio >= spike_multiplier or
                august_ratio >= spike_multiplier
            )

            # Must also meet minimum amount threshold
            if not is_suspicious or result.final_months_total < min_amount:
                continue

            agency = session.get(Agency, result.agency_id)
            if not agency:
                continue

            # Calculate what percentage of annual spending was in final months
            final_months_pct = (result.final_months_total / result.fy_total * 100) if result.fy_total else 0

            evidence = {
                "fiscal_year": fy,
                "agency_id": agency.id,
                "agency_name": agency.name,
                "fy_total_spending": float(result.fy_total),
                "final_months_spending": float(result.final_months_total),
                "final_months_percentage": round(float(final_months_pct), 1),
                "monthly_average": float(monthly_avg),
                "final_months_ratio": round(final_months_ratio, 2),
                "july_spending": float(result.july_total),
                "august_spending": float(result.august_total),
                "august_ratio": round(august_ratio, 2),
                "payment_count": result.payment_count,
            }

            # Severity based on ratio and amount
            severity = "medium"
            if august_ratio >= spike_multiplier * 1.5 or final_months_pct >= 35:
                severity = "high"

            alert_id = create_alert(
                alert_type="fiscal_year_spending_rush",
                severity=severity,
                title=f"FY{fy} year-end spending spike: {agency.name}",
                description=(
                    f"{agency.name} spent ${result.final_months_total:,.2f} "
                    f"in the final two months of FY{fy} ({final_months_pct:.1f}% of annual total). "
                    f"This is {final_months_ratio:.1f}x the monthly average of ${monthly_avg:,.2f}. "
                    f"August alone: ${result.august_total:,.2f} ({august_ratio:.1f}x average). "
                    f"This pattern may indicate 'use it or lose it' budget rushing."
                ),
                entity_type="agency",
                entity_id=agency.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _detect_vendor_fy_concentration(session) -> int:
    """Detect vendors who primarily receive payments at fiscal year end."""
    alerts_created = 0

    current_fy = to_state_fiscal_year(date.today())

    # Analyze vendors over last 3 fiscal years
    for fy in range(current_fy - 3, current_fy):
        fy_start = date(fy - 1, 9, 1)
        fy_end = date(fy, 8, 31)
        final_months_start = date(fy, 7, 1)

        # Find vendors with high concentration of payments in final months
        results = session.query(
            Payment.vendor_id,
            func.sum(Payment.amount).label("fy_total"),
            func.sum(
                case(
                    (Payment.payment_date >= final_months_start, Payment.amount),
                    else_=Decimal("0")
                )
            ).label("final_months_total"),
            func.count(Payment.id).label("payment_count"),
            func.count(
                case(
                    (Payment.payment_date >= final_months_start, Payment.id),
                    else_=None
                )
            ).label("final_months_count"),
        ).filter(
            Payment.payment_date >= fy_start,
            Payment.payment_date <= fy_end,
            Payment.vendor_id.isnot(None),
        ).group_by(
            Payment.vendor_id
        ).having(
            func.sum(Payment.amount) >= 50000  # Minimum threshold
        ).all()

        for result in results:
            if not result.fy_total or result.fy_total == 0:
                continue

            # Calculate concentration percentage
            final_months_pct = (result.final_months_total / result.fy_total * 100) if result.fy_total else 0

            # Flag if 70%+ of payments are in final months
            if final_months_pct < 70:
                continue

            vendor = session.get(Vendor, result.vendor_id)
            if not vendor:
                continue

            evidence = {
                "fiscal_year": fy,
                "vendor_id": vendor.id,
                "vendor_name": vendor.name,
                "fy_total": float(result.fy_total),
                "final_months_total": float(result.final_months_total),
                "final_months_percentage": round(float(final_months_pct), 1),
                "payment_count": result.payment_count,
                "final_months_payment_count": result.final_months_count,
            }

            severity = "medium"
            if final_months_pct >= 90 or float(result.fy_total) >= 500000:
                severity = "high"

            alert_id = create_alert(
                alert_type="vendor_fy_end_concentration",
                severity=severity,
                title=f"Vendor with FY{fy} year-end payment concentration: {vendor.name}",
                description=(
                    f"Vendor '{vendor.name}' received {final_months_pct:.0f}% "
                    f"(${result.final_months_total:,.2f} of ${result.fy_total:,.2f}) "
                    f"of their FY{fy} payments in the final two months. "
                    f"This unusual concentration may indicate coordination with agencies "
                    f"to spend remaining budget allocations."
                ),
                entity_type="vendor",
                entity_id=vendor.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _detect_fy_end_clusters(session, min_amount: Decimal) -> int:
    """Detect unusual clusters of large payments in final days of fiscal year."""
    alerts_created = 0

    current_fy = to_state_fiscal_year(date.today())

    # Check last few days of each fiscal year
    for fy in range(current_fy - 5, current_fy):
        # Last 5 days of Texas FY (Aug 27-31)
        final_days_start = date(fy, 8, 27)
        fy_end = date(fy, 8, 31)

        # Find agencies with large payments in final days
        results = session.query(
            Payment.agency_id,
            func.sum(Payment.amount).label("final_days_total"),
            func.count(Payment.id).label("payment_count"),
            func.count(Payment.vendor_id.distinct()).label("vendor_count"),
        ).filter(
            Payment.payment_date >= final_days_start,
            Payment.payment_date <= fy_end,
            Payment.agency_id.isnot(None),
        ).group_by(
            Payment.agency_id
        ).having(
            func.sum(Payment.amount) >= min_amount
        ).all()

        for result in results:
            agency = session.get(Agency, result.agency_id)
            if not agency:
                continue

            # Get vendor details
            vendors = session.query(
                Vendor.id,
                Vendor.name,
                func.sum(Payment.amount).label("total"),
            ).join(
                Payment, Payment.vendor_id == Vendor.id
            ).filter(
                Payment.payment_date >= final_days_start,
                Payment.payment_date <= fy_end,
                Payment.agency_id == result.agency_id,
            ).group_by(
                Vendor.id
            ).order_by(
                func.sum(Payment.amount).desc()
            ).limit(10).all()

            vendor_details = [
                {
                    "vendor_id": v.id,
                    "vendor_name": v.name,
                    "amount": float(v.total),
                }
                for v in vendors
            ]

            evidence = {
                "fiscal_year": fy,
                "agency_id": agency.id,
                "agency_name": agency.name,
                "final_days_total": float(result.final_days_total),
                "payment_count": result.payment_count,
                "vendor_count": result.vendor_count,
                "date_range": f"{final_days_start} to {fy_end}",
                "top_vendors": vendor_details,
            }

            severity = "medium"
            if float(result.final_days_total) >= 1000000 or result.payment_count >= 50:
                severity = "high"

            alert_id = create_alert(
                alert_type="fy_end_payment_cluster",
                severity=severity,
                title=f"FY{fy} last-minute payment cluster: {agency.name}",
                description=(
                    f"{agency.name} made {result.payment_count} payments totaling "
                    f"${result.final_days_total:,.2f} to {result.vendor_count} vendors "
                    f"in the final 5 days of FY{fy} (Aug 27-31). "
                    f"This last-minute spending rush may indicate poor planning or "
                    f"intentional budget exhaustion."
                ),
                entity_type="agency",
                entity_id=agency.id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created
