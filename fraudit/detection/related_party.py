"""
Related Party Network Detection.

Builds comprehensive related party networks using multiple data sources:
- Vendor address clustering
- Employee-vendor matches
- Campaign contributor matches
- Vendor relationship chains

Creates EntityMatch records and generates alerts for complex networks that
may indicate coordinated fraud, bid rigging, or conflicts of interest.

This module integrates findings from other detection modules to identify
larger patterns of related party transactions.
"""

from collections import defaultdict, deque
from decimal import Decimal

from sqlalchemy import func, or_, case

from fraudit.database import (
    get_session, Vendor, VendorRelationship, EntityMatch, Payment,
    Employee, CampaignContribution
)
from fraudit.normalization import normalize_vendor_name
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run related party network detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    min_network_size = thresholds.get("related_party_min_network_size", 3)
    min_network_value = Decimal(str(thresholds.get("related_party_min_value", 500000)))

    print(f"  Building related party networks (min size: {min_network_size})")

    with get_session() as session:
        # Build network from existing relationships
        print("    Analyzing vendor relationships...")
        alerts_created += _detect_vendor_networks(
            session, min_network_size, min_network_value
        )

        # Find employee-vendor-contributor triangles
        print("    Finding employee-vendor-contributor connections...")
        alerts_created += _detect_employee_vendor_contributor_links(session)

        # Detect circular payment patterns
        print("    Checking for circular payment patterns...")
        alerts_created += _detect_circular_patterns(session)

    return alerts_created


def _detect_vendor_networks(
    session,
    min_size: int,
    min_value: Decimal
) -> int:
    """Detect networks of related vendors using graph traversal."""
    alerts_created = 0

    # Build graph of vendor relationships
    relationships = session.query(VendorRelationship).all()
    entity_matches = session.query(EntityMatch).filter(
        EntityMatch.entity_type_1 == "vendor",
        EntityMatch.entity_type_2 == "vendor",
    ).all()

    # Build adjacency list
    graph = defaultdict(set)

    # Add vendor-vendor relationships
    for rel in relationships:
        graph[rel.vendor_id_1].add(rel.vendor_id_2)
        graph[rel.vendor_id_2].add(rel.vendor_id_1)

    # Add entity matches
    for match in entity_matches:
        graph[match.entity_id_1].add(match.entity_id_2)
        graph[match.entity_id_2].add(match.entity_id_1)

    # Find connected components using BFS
    visited = set()
    networks = []

    for vendor_id in graph.keys():
        if vendor_id in visited:
            continue

        # BFS to find connected component
        component = set()
        queue = deque([vendor_id])
        component.add(vendor_id)
        visited.add(vendor_id)

        while queue:
            current = queue.popleft()
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    component.add(neighbor)
                    queue.append(neighbor)

        if len(component) >= min_size:
            networks.append(component)

    # Analyze each network
    for network in networks:
        vendor_ids = list(network)

        # Get vendor details and payment totals
        vendors = session.query(
            Vendor.id,
            Vendor.name,
            Vendor.vendor_id,
            Vendor.address,
            func.sum(Payment.amount).label("total_payments"),
            func.count(Payment.id).label("payment_count"),
        ).outerjoin(
            Payment, Payment.vendor_id == Vendor.id
        ).filter(
            Vendor.id.in_(vendor_ids)
        ).group_by(
            Vendor.id
        ).all()

        # Calculate network statistics
        total_network_value = sum(
            float(v.total_payments or 0) for v in vendors
        )

        if Decimal(str(total_network_value)) < min_value:
            continue

        # Find relationship types within network
        network_relationships = session.query(VendorRelationship).filter(
            VendorRelationship.vendor_id_1.in_(vendor_ids),
            VendorRelationship.vendor_id_2.in_(vendor_ids),
        ).all()

        relationship_types = defaultdict(int)
        for rel in network_relationships:
            relationship_types[rel.relationship_type] += 1

        # Find entity matches within network
        network_matches = session.query(EntityMatch).filter(
            EntityMatch.entity_type_1 == "vendor",
            EntityMatch.entity_type_2 == "vendor",
            EntityMatch.entity_id_1.in_(vendor_ids),
            EntityMatch.entity_id_2.in_(vendor_ids),
        ).all()

        match_types = defaultdict(int)
        for match in network_matches:
            match_types[match.match_type] += 1

        # Build evidence
        vendor_details = [
            {
                "id": v.id,
                "name": v.name,
                "vendor_id": v.vendor_id,
                "address": v.address,
                "total_payments": float(v.total_payments or 0),
                "payment_count": v.payment_count or 0,
            }
            for v in vendors
        ]

        evidence = {
            "network_size": len(network),
            "total_network_value": total_network_value,
            "vendors": vendor_details,
            "relationship_types": dict(relationship_types),
            "match_types": dict(match_types),
            "relationship_count": len(network_relationships) + len(network_matches),
        }

        # Severity based on network size, value, and complexity
        severity = "medium"
        if len(network) >= 5 or total_network_value >= 2000000:
            severity = "high"

        # Especially suspicious if multiple relationship types
        if len(relationship_types) + len(match_types) >= 3:
            severity = "high"

        alert_id = create_alert(
            alert_type="related_party_network",
            severity=severity,
            title=f"Related party network ({len(network)} vendors, ${total_network_value:,.0f})",
            description=(
                f"Found network of {len(network)} related vendors with "
                f"combined payments of ${total_network_value:,.2f}. "
                f"Relationships: {', '.join(f'{k}({v})' for k, v in relationship_types.items())}. "
                f"Matches: {', '.join(f'{k}({v})' for k, v in match_types.items())}. "
                f"This network may indicate coordinated activity, shell companies, "
                f"or bid rigging."
            ),
            entity_type="vendor",
            entity_id=vendor_ids[0] if vendor_ids else None,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_employee_vendor_contributor_links(session) -> int:
    """Detect triangular relationships: employee-vendor-campaign contributor."""
    from rapidfuzz import fuzz, process

    alerts_created = 0

    # Find employee-vendor matches
    employee_vendor_matches = session.query(EntityMatch).filter(
        EntityMatch.entity_type_1 == "employee",
        EntityMatch.entity_type_2 == "vendor",
    ).all()

    if not employee_vendor_matches:
        return 0

    # Pre-load all employees and vendors for these matches
    match_employee_ids = [m.entity_id_1 for m in employee_vendor_matches]
    match_vendor_ids = [m.entity_id_2 for m in employee_vendor_matches]

    employees_by_id = {e.id: e for e in session.query(Employee).filter(
        Employee.id.in_(match_employee_ids)
    ).all()}

    vendors_by_id = {v.id: v for v in session.query(Vendor).filter(
        Vendor.id.in_(match_vendor_ids)
    ).all()}

    # Pre-load all campaign contributions grouped by normalized name
    contributions_by_name = defaultdict(list)
    all_contributions = session.query(CampaignContribution).filter(
        CampaignContribution.contributor_normalized.isnot(None)
    ).all()

    for c in all_contributions:
        contributions_by_name[c.contributor_normalized].append(c)

    contributor_names = list(contributions_by_name.keys())

    # Pre-compute payment stats for all matched vendors
    payment_stats = dict(
        session.query(
            Payment.vendor_id,
            func.sum(Payment.amount).label("total"),
            func.count(Payment.id).label("count"),
        ).filter(
            Payment.vendor_id.in_(match_vendor_ids)
        ).group_by(Payment.vendor_id).all()
    )

    print(f"      Checking {len(employee_vendor_matches):,} matches against {len(contributor_names):,} contributors...")

    for match in employee_vendor_matches:
        employee = employees_by_id.get(match.entity_id_1)
        vendor = vendors_by_id.get(match.entity_id_2)

        if not employee or not vendor:
            continue

        # Use fuzzy matching to find similar contributor names
        vendor_name_normalized = normalize_vendor_name(vendor.name)

        # Find matching contributor names
        fuzzy_matches = process.extract(
            vendor_name_normalized,
            contributor_names,
            scorer=fuzz.ratio,
            score_cutoff=80,  # Lower threshold to catch variations
            limit=5,
        )

        if not fuzzy_matches:
            continue

        # Gather all contributions from matching names
        matching_contributions = []
        for contrib_name, score, _ in fuzzy_matches:
            matching_contributions.extend(contributions_by_name[contrib_name])

        if not matching_contributions:
            continue

        # Calculate total contributions
        total_contributions = sum(
            float(c.contribution_amount or 0) for c in matching_contributions
        )

        # Get pre-computed payment stats
        stats = payment_stats.get(vendor.id)
        if not stats or not stats.total:
            continue

        # Find which officials received contributions
        recipients = defaultdict(Decimal)
        for c in matching_contributions:
            recipients[c.filer_name] += c.contribution_amount or Decimal("0")

        evidence = {
            "employee_id": employee.id,
            "employee_name": employee.name,
            "employee_agency": employee.agency.name if employee.agency else None,
            "employee_title": employee.job_title,
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "match_type": match.match_type,
            "match_confidence": float(match.confidence_score or 0),
            "vendor_payments": float(stats.total),
            "payment_count": stats.count,
            "contribution_count": len(matching_contributions),
            "total_contributions": total_contributions,
            "contribution_recipients": [
                {"name": name, "amount": float(amount)}
                for name, amount in sorted(
                    recipients.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]
            ],
        }

        severity = "medium"
        if total_contributions >= 10000 or float(stats.total) >= 500000:
            severity = "high"

        alert_id = create_alert(
            alert_type="employee_vendor_contributor_triangle",
            severity=severity,
            title=f"Employee-vendor-contributor link: {employee.name}",
            description=(
                f"Employee '{employee.name}' is linked to vendor '{vendor.name}' "
                f"(match type: {match.match_type}, confidence: {match.confidence_score:.0%}). "
                f"The vendor has received ${stats.total:,.2f} in payments "
                f"and appears to have made ${total_contributions:,.2f} in campaign contributions. "
                f"This triangular relationship warrants investigation for potential "
                f"conflicts of interest or pay-to-play schemes."
            ),
            entity_type="employee",
            entity_id=employee.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_circular_patterns(session) -> int:
    """Detect potential circular payment patterns between related entities."""
    from fraudit.database import Agency

    alerts_created = 0

    # Get all high-confidence vendor relationships
    relationships = session.query(VendorRelationship).filter(
        VendorRelationship.confidence_score >= 0.7
    ).all()

    if not relationships:
        return 0

    # Collect all vendor IDs from relationships
    all_vendor_ids = set()
    for rel in relationships:
        all_vendor_ids.add(rel.vendor_id_1)
        all_vendor_ids.add(rel.vendor_id_2)

    if not all_vendor_ids:
        return 0

    print(f"      Analyzing {len(relationships):,} vendor relationships...")

    # Pre-load all vendors
    vendors_by_id = {v.id: v for v in session.query(Vendor).filter(
        Vendor.id.in_(all_vendor_ids)
    ).all()}

    # Pre-load all agencies
    agencies = session.query(Agency).all()
    agencies_by_id = {a.id: a for a in agencies}

    # Pre-compute: for each vendor, which agencies paid them and how much
    # {vendor_id: {agency_id: total_amount}}
    vendor_agency_payments = defaultdict(lambda: defaultdict(Decimal))

    payments = session.query(
        Payment.vendor_id,
        Payment.agency_id,
        func.sum(Payment.amount).label("total")
    ).filter(
        Payment.vendor_id.in_(all_vendor_ids),
        Payment.agency_id.isnot(None),
    ).group_by(
        Payment.vendor_id, Payment.agency_id
    ).all()

    for p in payments:
        vendor_agency_payments[p.vendor_id][p.agency_id] = p.total

    # Now analyze each relationship using pre-computed data
    for rel in relationships:
        vendor1 = vendors_by_id.get(rel.vendor_id_1)
        vendor2 = vendors_by_id.get(rel.vendor_id_2)

        if not vendor1 or not vendor2:
            continue

        # Find common agencies (agencies that paid both)
        v1_agencies = set(vendor_agency_payments[vendor1.id].keys())
        v2_agencies = set(vendor_agency_payments[vendor2.id].keys())
        common_agency_ids = v1_agencies & v2_agencies

        if not common_agency_ids:
            continue

        # Calculate totals
        total_v1 = sum(float(vendor_agency_payments[vendor1.id][aid]) for aid in common_agency_ids)
        total_v2 = sum(float(vendor_agency_payments[vendor2.id][aid]) for aid in common_agency_ids)

        # Must be significant amounts
        if total_v1 < 50000 or total_v2 < 50000:
            continue

        # Build agency details
        agency_details = []
        for aid in common_agency_ids:
            agency = agencies_by_id.get(aid)
            if agency:
                agency_details.append({
                    "name": agency.name,
                    "vendor1_payments": float(vendor_agency_payments[vendor1.id][aid]),
                    "vendor2_payments": float(vendor_agency_payments[vendor2.id][aid]),
                })

        evidence = {
            "vendor1_id": vendor1.id,
            "vendor1_name": vendor1.name,
            "vendor2_id": vendor2.id,
            "vendor2_name": vendor2.name,
            "relationship_type": rel.relationship_type,
            "relationship_confidence": float(rel.confidence_score or 0),
            "common_agency_count": len(common_agency_ids),
            "vendor1_total": total_v1,
            "vendor2_total": total_v2,
            "agencies": agency_details,
        }

        severity = "medium"
        if len(common_agency_ids) >= 3 or (total_v1 + total_v2) >= 1000000:
            severity = "high"

        alert_id = create_alert(
            alert_type="circular_payment_pattern",
            severity=severity,
            title=f"Circular payment pattern: {vendor1.name} & {vendor2.name}",
            description=(
                f"Related vendors '{vendor1.name}' and '{vendor2.name}' "
                f"({rel.relationship_type}) both receive payments from "
                f"{len(common_agency_ids)} common agencies. "
                f"Vendor 1: ${total_v1:,.2f}, Vendor 2: ${total_v2:,.2f}. "
                f"This pattern may indicate bid rotation, market allocation, "
                f"or coordinated fraud."
            ),
            entity_type="vendor",
            entity_id=vendor1.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created
