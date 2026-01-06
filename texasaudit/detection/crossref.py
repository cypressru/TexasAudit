"""
Cross-referencing module to link entities across data sources.

This module finds connections between:
- Vendors and employees (insider dealing)
- Vendors and campaign contributors (pay-to-play)
- Vendors and tax permits (legitimacy verification)
- Vendors and lobbying activities (influence detection)
"""

from collections import defaultdict
from datetime import datetime
from typing import Optional

from rapidfuzz import fuzz
from sqlalchemy import func, and_, or_
from tqdm import tqdm

from texasaudit.database import get_session, Vendor, Alert, AlertSeverity, AlertStatus
from texasaudit.normalization import normalize_vendor_name, normalize_address


# Attempt to import new models - they may not exist yet
try:
    from texasaudit.database import Employee, CampaignContribution, TaxPermit, LobbyingActivity, EntityMatch
    EXTENDED_MODELS = True
except ImportError:
    EXTENDED_MODELS = False


def detect(thresholds: dict) -> int:
    """Run all cross-referencing detection."""
    alerts = 0

    # Always run vendor-to-vendor matching (uses existing models)
    alerts += detect_vendor_address_clusters(thresholds)

    if EXTENDED_MODELS:
        alerts += detect_employee_vendor_matches(thresholds)
        alerts += detect_campaign_vendor_matches(thresholds)
        alerts += detect_unregistered_vendors(thresholds)

    return alerts


def detect_vendor_address_clusters(thresholds: dict) -> int:
    """
    Find vendors sharing the same address.

    This can indicate:
    - Shell companies at the same location
    - Related parties trying to appear separate
    - Contract splitting across multiple "vendors"
    """
    print("  Analyzing vendor address clusters...")
    alerts_created = 0

    with get_session() as session:
        # Find addresses with multiple vendors
        address_counts = session.query(
            Vendor.address,
            Vendor.city,
            Vendor.state,
            func.count(Vendor.id).label('vendor_count')
        ).filter(
            Vendor.address.isnot(None),
            Vendor.address != '',
            Vendor.city.isnot(None)
        ).group_by(
            Vendor.address, Vendor.city, Vendor.state
        ).having(
            func.count(Vendor.id) >= 3  # At least 3 vendors at same address
        ).all()

        for address, city, state, count in tqdm(address_counts, desc="Address clusters"):
            # Get all vendors at this address
            vendors = session.query(Vendor).filter(
                Vendor.address == address,
                Vendor.city == city,
                Vendor.state == state
            ).all()

            if len(vendors) < 3:
                continue

            # Calculate total spending across these vendors
            from texasaudit.database import Payment, Contract
            total_payments = 0
            total_contracts = 0
            vendor_ids = [v.id for v in vendors]

            for v in vendors:
                payments = session.query(func.sum(Payment.amount)).filter(
                    Payment.vendor_id == v.id
                ).scalar() or 0
                contracts = session.query(func.sum(Contract.current_value)).filter(
                    Contract.vendor_id == v.id
                ).scalar() or 0
                total_payments += float(payments)
                total_contracts += float(contracts)

            # Check if alert already exists
            existing = session.query(Alert).filter(
                Alert.alert_type == "address_cluster",
                Alert.description.contains(address[:50])
            ).first()

            if existing:
                continue

            # Determine severity based on spending
            combined = total_payments + total_contracts
            if combined > 1000000:
                severity = AlertSeverity.HIGH
            elif combined > 100000:
                severity = AlertSeverity.MEDIUM
            else:
                severity = AlertSeverity.LOW

            # Create alert
            alert = Alert(
                alert_type="address_cluster",
                severity=severity,
                title=f"{count} vendors share address: {address[:40]}",
                description=f"Found {count} vendors operating from the same address: {address}, {city}, {state}. Combined spending: ${combined:,.0f}",
                entity_type="vendor",
                entity_id=vendors[0].id,
                evidence={
                    "address": address,
                    "city": city,
                    "state": state,
                    "vendor_count": count,
                    "vendor_names": [v.name for v in vendors[:10]],
                    "vendor_ids": [v.vendor_id for v in vendors[:10]],
                    "total_payments": total_payments,
                    "total_contracts": total_contracts,
                },
                status=AlertStatus.NEW,
            )
            session.add(alert)
            alerts_created += 1

        session.commit()

    print(f"    Created {alerts_created} address cluster alerts")
    return alerts_created


def detect_employee_vendor_matches(thresholds: dict) -> int:
    """
    Find employees whose names match vendor names.

    This can indicate:
    - State employees running businesses that contract with the state
    - Family members of employees receiving contracts
    - Potential conflicts of interest
    """
    if not EXTENDED_MODELS:
        return 0

    print("  Analyzing employee-vendor matches...")
    alerts_created = 0
    min_score = thresholds.get("name_match_threshold", 90)

    with get_session() as session:
        # Get all employees
        employees = session.query(Employee).all()

        if not employees:
            print("    No employee data available")
            return 0

        # Get vendors with significant payments/contracts
        vendors = session.query(Vendor).filter(
            or_(
                Vendor.in_cmbl == True,
                Vendor.risk_score.isnot(None)
            )
        ).all()

        for emp in tqdm(employees, desc="Employee matches"):
            emp_name_norm = emp.name_normalized or normalize_vendor_name(emp.name)

            for vendor in vendors:
                vendor_name_norm = vendor.name_normalized or normalize_vendor_name(vendor.name)

                # Fuzzy match on normalized names
                score = fuzz.ratio(emp_name_norm, vendor_name_norm)

                if score >= min_score:
                    # Check if match already recorded
                    existing = session.query(EntityMatch).filter(
                        EntityMatch.entity_type_1 == "employee",
                        EntityMatch.entity_id_1 == emp.id,
                        EntityMatch.entity_type_2 == "vendor",
                        EntityMatch.entity_id_2 == vendor.id,
                    ).first()

                    if existing:
                        continue

                    # Record the match
                    match = EntityMatch(
                        entity_type_1="employee",
                        entity_id_1=emp.id,
                        entity_type_2="vendor",
                        entity_id_2=vendor.id,
                        match_type="name",
                        confidence_score=score / 100.0,
                        evidence={
                            "employee_name": emp.name,
                            "vendor_name": vendor.name,
                            "employee_agency": emp.agency.name if emp.agency else None,
                            "employee_title": emp.job_title,
                            "employee_salary": float(emp.annual_salary) if emp.annual_salary else None,
                            "match_score": score,
                        },
                        is_confirmed=False,
                    )
                    session.add(match)

                    # Create alert for high-confidence matches
                    if score >= 95:
                        alert = Alert(
                            alert_type="employee_vendor_match",
                            severity=AlertSeverity.HIGH,
                            title=f"Employee name matches vendor: {emp.name[:30]}",
                            description=f"State employee '{emp.name}' ({emp.job_title}) has {score}% name match with vendor '{vendor.name}'",
                            entity_type="vendor",
                            entity_id=vendor.id,
                            evidence=match.evidence,
                            status=AlertStatus.NEW,
                        )
                        session.add(alert)
                        alerts_created += 1

        session.commit()

    print(f"    Created {alerts_created} employee-vendor match alerts")
    return alerts_created


def detect_campaign_vendor_matches(thresholds: dict) -> int:
    """
    Find campaign contributors who are also state vendors.

    This can indicate:
    - Pay-to-play arrangements
    - Quid pro quo contracts
    - Political influence on procurement
    """
    if not EXTENDED_MODELS:
        return 0

    print("  Analyzing campaign contributor-vendor matches...")
    alerts_created = 0
    min_score = thresholds.get("name_match_threshold", 90)
    min_contribution = thresholds.get("min_contribution_for_alert", 5000)

    with get_session() as session:
        # Get significant contributors
        contributors = session.query(
            CampaignContribution.contributor_normalized,
            func.sum(CampaignContribution.contribution_amount).label('total'),
            func.array_agg(CampaignContribution.filer_name).label('recipients')
        ).filter(
            CampaignContribution.contribution_amount >= 500
        ).group_by(
            CampaignContribution.contributor_normalized
        ).having(
            func.sum(CampaignContribution.contribution_amount) >= min_contribution
        ).all()

        if not contributors:
            print("    No campaign contribution data available")
            return 0

        # Get all vendors
        vendors = session.query(Vendor).filter(
            Vendor.name_normalized.isnot(None)
        ).all()

        vendor_lookup = {v.name_normalized: v for v in vendors if v.name_normalized}

        for contrib_name, total_contrib, recipients in tqdm(contributors, desc="Contributor matches"):
            if not contrib_name:
                continue

            for vendor_name_norm, vendor in vendor_lookup.items():
                score = fuzz.ratio(contrib_name, vendor_name_norm)

                if score >= min_score:
                    # Check for existing match
                    existing = session.query(Alert).filter(
                        Alert.alert_type == "pay_to_play",
                        Alert.entity_id == vendor.id,
                        Alert.evidence['contributor_name'].astext == contrib_name
                    ).first()

                    if existing:
                        continue

                    # Get vendor's contract/payment amounts
                    from texasaudit.database import Payment, Contract
                    vendor_payments = session.query(func.sum(Payment.amount)).filter(
                        Payment.vendor_id == vendor.id
                    ).scalar() or 0
                    vendor_contracts = session.query(func.sum(Contract.current_value)).filter(
                        Contract.vendor_id == vendor.id
                    ).scalar() or 0

                    # Calculate ROI (contracts received / contributions made)
                    roi = (float(vendor_payments) + float(vendor_contracts)) / float(total_contrib) if total_contrib > 0 else 0

                    # Higher ROI = higher severity
                    if roi > 100:
                        severity = AlertSeverity.HIGH
                    elif roi > 10:
                        severity = AlertSeverity.MEDIUM
                    else:
                        severity = AlertSeverity.LOW

                    alert = Alert(
                        alert_type="pay_to_play",
                        severity=severity,
                        title=f"Campaign contributor is state vendor: {vendor.name[:30]}",
                        description=f"Campaign contributor '{contrib_name}' (${float(total_contrib):,.0f} donated) matches vendor '{vendor.name}' (${float(vendor_payments + vendor_contracts):,.0f} in state business). ROI: {roi:.1f}x",
                        entity_type="vendor",
                        entity_id=vendor.id,
                        evidence={
                            "contributor_name": contrib_name,
                            "vendor_name": vendor.name,
                            "total_contributions": float(total_contrib),
                            "recipients": list(set(recipients))[:10],
                            "vendor_payments": float(vendor_payments),
                            "vendor_contracts": float(vendor_contracts),
                            "match_score": score,
                            "roi": roi,
                        },
                        status=AlertStatus.NEW,
                    )
                    session.add(alert)
                    alerts_created += 1

        session.commit()

    print(f"    Created {alerts_created} pay-to-play alerts")
    return alerts_created


def detect_unregistered_vendors(thresholds: dict) -> int:
    """
    Find vendors receiving payments but not registered with tax permits.

    This can indicate:
    - Ghost vendors (fake companies)
    - Out-of-state companies evading Texas taxes
    - Unregistered businesses
    """
    if not EXTENDED_MODELS:
        return 0

    print("  Analyzing unregistered vendors...")
    alerts_created = 0
    min_amount = thresholds.get("ghost_vendor_min_amount", 10000)

    with get_session() as session:
        # Get vendors not in CMBL with significant payments
        from texasaudit.database import Payment

        suspicious_vendors = session.query(
            Vendor,
            func.sum(Payment.amount).label('total_payments')
        ).join(Payment).filter(
            Vendor.in_cmbl == False,
            Vendor.state == 'TX'  # Focus on Texas vendors
        ).group_by(Vendor.id).having(
            func.sum(Payment.amount) >= min_amount
        ).all()

        # Get all tax permit taxpayer names for matching
        permits = session.query(TaxPermit.taxpayer_normalized).filter(
            TaxPermit.taxpayer_normalized.isnot(None)
        ).distinct().all()
        permit_names = {p[0] for p in permits}

        for vendor, total_payments in tqdm(suspicious_vendors, desc="Unregistered vendors"):
            vendor_name_norm = vendor.name_normalized or normalize_vendor_name(vendor.name)

            # Check if vendor matches any tax permit
            has_permit = False
            for permit_name in permit_names:
                if fuzz.ratio(vendor_name_norm, permit_name) >= 90:
                    has_permit = True
                    break

            if not has_permit:
                # Check for existing alert
                existing = session.query(Alert).filter(
                    Alert.alert_type == "ghost_vendor",
                    Alert.entity_id == vendor.id,
                ).first()

                if existing:
                    continue

                # Determine severity by payment amount
                if float(total_payments) > 100000:
                    severity = AlertSeverity.HIGH
                elif float(total_payments) > 25000:
                    severity = AlertSeverity.MEDIUM
                else:
                    severity = AlertSeverity.LOW

                alert = Alert(
                    alert_type="ghost_vendor",
                    severity=severity,
                    title=f"Unregistered vendor: {vendor.name[:30]}",
                    description=f"Vendor '{vendor.name}' received ${float(total_payments):,.0f} but is not in CMBL and has no matching tax permit",
                    entity_type="vendor",
                    entity_id=vendor.id,
                    evidence={
                        "vendor_name": vendor.name,
                        "vendor_id": vendor.vendor_id,
                        "address": vendor.address,
                        "city": vendor.city,
                        "state": vendor.state,
                        "total_payments": float(total_payments),
                        "in_cmbl": vendor.in_cmbl,
                        "has_tax_permit": False,
                    },
                    status=AlertStatus.NEW,
                )
                session.add(alert)
                alerts_created += 1

        session.commit()

    print(f"    Created {alerts_created} ghost vendor alerts")
    return alerts_created


def build_entity_network(vendor_id: int) -> dict:
    """
    Build a network of related entities for a specific vendor.

    Returns a dict with:
    - related_vendors: vendors at same address or with similar names
    - related_employees: employees matching vendor name
    - related_contributors: campaign contributors matching vendor
    - related_contracts: contracts with this vendor
    - risk_indicators: list of risk factors found
    """
    network = {
        "vendor_id": vendor_id,
        "related_vendors": [],
        "related_employees": [],
        "related_contributors": [],
        "related_contracts": [],
        "risk_indicators": [],
    }

    with get_session() as session:
        vendor = session.query(Vendor).filter(Vendor.id == vendor_id).first()
        if not vendor:
            return network

        # Find related vendors by address
        if vendor.address:
            related = session.query(Vendor).filter(
                Vendor.id != vendor.id,
                Vendor.address == vendor.address,
                Vendor.city == vendor.city,
            ).all()

            for v in related:
                network["related_vendors"].append({
                    "id": v.id,
                    "name": v.name,
                    "relationship": "same_address",
                })

            if len(related) >= 2:
                network["risk_indicators"].append(f"Shares address with {len(related)} other vendors")

        # Find related vendors by similar name
        vendor_name_norm = vendor.name_normalized or normalize_vendor_name(vendor.name)
        all_vendors = session.query(Vendor).filter(
            Vendor.id != vendor.id,
            Vendor.name_normalized.isnot(None)
        ).all()

        for v in all_vendors:
            score = fuzz.ratio(vendor_name_norm, v.name_normalized)
            if score >= 85 and score < 100:
                network["related_vendors"].append({
                    "id": v.id,
                    "name": v.name,
                    "relationship": "similar_name",
                    "score": score,
                })

        # Find related employees if available
        if EXTENDED_MODELS:
            matches = session.query(EntityMatch).filter(
                EntityMatch.entity_type_2 == "vendor",
                EntityMatch.entity_id_2 == vendor_id,
                EntityMatch.entity_type_1 == "employee",
            ).all()

            for match in matches:
                emp = session.query(Employee).filter(Employee.id == match.entity_id_1).first()
                if emp:
                    network["related_employees"].append({
                        "id": emp.id,
                        "name": emp.name,
                        "agency": emp.agency.name if emp.agency else None,
                        "title": emp.job_title,
                        "confidence": match.confidence_score,
                    })
                    network["risk_indicators"].append(f"Name matches state employee: {emp.name}")

        # Get contracts
        from texasaudit.database import Contract
        contracts = session.query(Contract).filter(Contract.vendor_id == vendor_id).all()
        for c in contracts:
            network["related_contracts"].append({
                "id": c.id,
                "number": c.contract_number,
                "value": float(c.current_value) if c.current_value else 0,
                "agency": c.agency.name if c.agency else None,
            })

        # Check for alerts
        alerts = session.query(Alert).filter(
            Alert.entity_id == vendor_id,
            Alert.entity_type == "vendor",
        ).all()

        for alert in alerts:
            network["risk_indicators"].append(f"{alert.severity.value.upper()}: {alert.title}")

    return network
