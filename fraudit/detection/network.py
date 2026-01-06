"""
Network Analysis for Fraud Detection.

Uses graph analysis to detect:
- Unusual vendor-agency relationship concentrations
- Circular payment patterns
- Hidden connections through shared entities
- Suspicious bidding patterns
"""

from collections import defaultdict
from decimal import Decimal

import networkx as nx
from sqlalchemy import func

from fraudit.database import (
    get_session, Payment, Vendor, Contract, Agency, VendorRelationship
)
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run network analysis detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    print("  Building vendor-agency network...")

    with get_session() as session:
        # Build the network graph
        G = build_network_graph(session)

        if G.number_of_nodes() < 10:
            print("    Insufficient data for network analysis")
            return 0

        # Analyze network for suspicious patterns
        alerts_created += _detect_hub_vendors(session, G)
        alerts_created += _detect_isolated_clusters(session, G)
        alerts_created += _detect_exclusive_relationships(session, G)

    return alerts_created


def build_network_graph(session) -> nx.Graph:
    """
    Build a bipartite graph of vendor-agency relationships.

    Nodes: Vendors and Agencies
    Edges: Payments and contracts between them
    """
    G = nx.Graph()

    # Add vendor nodes
    vendors = session.query(Vendor).all()
    for v in vendors:
        G.add_node(
            f"V_{v.id}",
            type="vendor",
            name=v.name,
            vendor_id=v.vendor_id,
            db_id=v.id,
        )

    # Add agency nodes
    agencies = session.query(Agency).all()
    for a in agencies:
        G.add_node(
            f"A_{a.id}",
            type="agency",
            name=a.name,
            db_id=a.id,
        )

    # Add edges from payments
    payment_edges = session.query(
        Payment.vendor_id,
        Payment.agency_id,
        func.count(Payment.id).label("count"),
        func.sum(Payment.amount).label("total"),
    ).filter(
        Payment.vendor_id.isnot(None),
        Payment.agency_id.isnot(None),
    ).group_by(
        Payment.vendor_id,
        Payment.agency_id,
    ).all()

    for edge in payment_edges:
        v_node = f"V_{edge.vendor_id}"
        a_node = f"A_{edge.agency_id}"

        if G.has_node(v_node) and G.has_node(a_node):
            if G.has_edge(v_node, a_node):
                # Update existing edge
                G[v_node][a_node]["payment_count"] += edge.count
                G[v_node][a_node]["payment_total"] += float(edge.total or 0)
            else:
                G.add_edge(
                    v_node, a_node,
                    payment_count=edge.count,
                    payment_total=float(edge.total or 0),
                    contract_count=0,
                    contract_value=0,
                )

    # Add edges from contracts
    contract_edges = session.query(
        Contract.vendor_id,
        Contract.agency_id,
        func.count(Contract.id).label("count"),
        func.sum(Contract.current_value).label("total"),
    ).filter(
        Contract.vendor_id.isnot(None),
        Contract.agency_id.isnot(None),
    ).group_by(
        Contract.vendor_id,
        Contract.agency_id,
    ).all()

    for edge in contract_edges:
        v_node = f"V_{edge.vendor_id}"
        a_node = f"A_{edge.agency_id}"

        if G.has_node(v_node) and G.has_node(a_node):
            if G.has_edge(v_node, a_node):
                G[v_node][a_node]["contract_count"] = edge.count
                G[v_node][a_node]["contract_value"] = float(edge.total or 0)
            else:
                G.add_edge(
                    v_node, a_node,
                    payment_count=0,
                    payment_total=0,
                    contract_count=edge.count,
                    contract_value=float(edge.total or 0),
                )

    # Add vendor-vendor edges from relationships
    relationships = session.query(VendorRelationship).all()
    for rel in relationships:
        v1_node = f"V_{rel.vendor_id_1}"
        v2_node = f"V_{rel.vendor_id_2}"

        if G.has_node(v1_node) and G.has_node(v2_node):
            G.add_edge(
                v1_node, v2_node,
                relationship_type=rel.relationship_type,
                confidence=float(rel.confidence_score or 0),
            )

    return G


def _detect_hub_vendors(session, G: nx.Graph) -> int:
    """Find vendors with unusually many agency relationships."""
    alerts_created = 0

    # Get vendor nodes and their degree (number of agency connections)
    vendor_degrees = []
    for node in G.nodes():
        if not node.startswith("V_"):
            continue

        # Count only agency connections
        agency_neighbors = sum(
            1 for n in G.neighbors(node) if n.startswith("A_")
        )
        if agency_neighbors > 0:
            vendor_degrees.append((node, agency_neighbors))

    if not vendor_degrees:
        return 0

    # Calculate statistics
    degrees = [d for _, d in vendor_degrees]
    mean_degree = sum(degrees) / len(degrees)
    std_degree = (sum((d - mean_degree) ** 2 for d in degrees) / len(degrees)) ** 0.5

    # Flag vendors with degree > mean + 2*std (unusually many connections)
    threshold = mean_degree + 2 * std_degree

    for node, degree in vendor_degrees:
        if degree <= threshold or degree < 10:
            continue

        vendor_id = int(node.split("_")[1])
        vendor = session.get(Vendor, vendor_id)
        if not vendor:
            continue

        # Get agency details
        agencies = []
        total_value = 0
        for neighbor in G.neighbors(node):
            if neighbor.startswith("A_"):
                edge_data = G[node][neighbor]
                agency_data = G.nodes[neighbor]
                agencies.append({
                    "name": agency_data.get("name"),
                    "payment_total": edge_data.get("payment_total", 0),
                    "contract_count": edge_data.get("contract_count", 0),
                })
                total_value += edge_data.get("payment_total", 0)

        evidence = {
            "vendor_id": vendor.id,
            "vendor_name": vendor.name,
            "agency_count": degree,
            "mean_vendor_agencies": round(mean_degree, 1),
            "total_value": total_value,
            "agencies": agencies[:20],  # Top 20
        }

        severity = "low"
        if degree >= mean_degree + 3 * std_degree:
            severity = "medium"

        alert_id = create_alert(
            alert_type="hub_vendor",
            severity=severity,
            title=f"Hub vendor: {vendor.name}",
            description=(
                f"Vendor '{vendor.name}' has relationships with {degree} agencies "
                f"(average vendor: {mean_degree:.1f} agencies). "
                f"Total value: ${total_value:,.2f}. "
                f"This high connectivity may warrant review."
            ),
            entity_type="vendor",
            entity_id=vendor.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_isolated_clusters(session, G: nx.Graph) -> int:
    """Find isolated vendor clusters that only work together."""
    alerts_created = 0

    # Find connected components that are relatively isolated
    # Focus on vendor-vendor relationships

    # Create subgraph of just vendor-vendor edges
    vendor_nodes = [n for n in G.nodes() if n.startswith("V_")]
    vendor_edges = [
        (u, v) for u, v in G.edges()
        if u.startswith("V_") and v.startswith("V_")
    ]

    if not vendor_edges:
        return 0

    V = G.subgraph(vendor_nodes).copy()

    # Find connected components
    components = list(nx.connected_components(V))

    for component in components:
        if len(component) < 3 or len(component) > 20:
            continue

        # Check if this cluster shares agencies
        shared_agencies = defaultdict(int)
        total_value = 0

        for v_node in component:
            for neighbor in G.neighbors(v_node):
                if neighbor.startswith("A_"):
                    shared_agencies[neighbor] += 1
                    edge_data = G[v_node][neighbor]
                    total_value += edge_data.get("payment_total", 0)

        # Find agencies that work with multiple vendors in this cluster
        common_agencies = {a: c for a, c in shared_agencies.items() if c >= 2}

        if not common_agencies:
            continue

        # Get vendor details
        vendors = []
        for v_node in component:
            v_id = int(v_node.split("_")[1])
            vendor = session.get(Vendor, v_id)
            if vendor:
                vendors.append({
                    "id": vendor.id,
                    "name": vendor.name,
                    "vendor_id": vendor.vendor_id,
                })

        # Get common agency details
        agencies = []
        for a_node, count in common_agencies.items():
            agency_data = G.nodes.get(a_node, {})
            agencies.append({
                "name": agency_data.get("name"),
                "vendor_count": count,
            })

        evidence = {
            "vendor_count": len(component),
            "vendors": vendors,
            "common_agencies": agencies,
            "total_value": total_value,
        }

        severity = "medium"
        if len(component) >= 5 and total_value >= 1000000:
            severity = "high"

        alert_id = create_alert(
            alert_type="vendor_cluster",
            severity=severity,
            title=f"Related vendor cluster ({len(component)} vendors)",
            description=(
                f"Found cluster of {len(component)} related vendors sharing "
                f"{len(common_agencies)} common agencies. "
                f"Total combined payments: ${total_value:,.2f}. "
                f"This pattern may indicate coordinated activity."
            ),
            entity_type="vendor",
            entity_id=vendors[0]["id"] if vendors else None,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def _detect_exclusive_relationships(session, G: nx.Graph) -> int:
    """Find agencies that work almost exclusively with single vendors."""
    alerts_created = 0

    for node in G.nodes():
        if not node.startswith("A_"):
            continue

        # Get all vendor relationships
        vendors = []
        total_value = 0

        for neighbor in G.neighbors(node):
            if neighbor.startswith("V_"):
                edge_data = G[node][neighbor]
                payment_total = edge_data.get("payment_total", 0)
                vendors.append((neighbor, payment_total))
                total_value += payment_total

        if not vendors or total_value < 100000:
            continue

        # Sort by value
        vendors.sort(key=lambda x: x[1], reverse=True)

        # Check if top vendor dominates
        top_vendor_value = vendors[0][1]
        top_vendor_share = top_vendor_value / total_value if total_value else 0

        if top_vendor_share < 0.80:  # Less than 80% to single vendor
            continue

        agency_id = int(node.split("_")[1])
        agency = session.get(Agency, agency_id)
        if not agency:
            continue

        top_vendor_id = int(vendors[0][0].split("_")[1])
        top_vendor = session.get(Vendor, top_vendor_id)

        evidence = {
            "agency_id": agency.id,
            "agency_name": agency.name,
            "top_vendor_id": top_vendor.id if top_vendor else None,
            "top_vendor_name": top_vendor.name if top_vendor else None,
            "top_vendor_share": round(top_vendor_share * 100, 1),
            "top_vendor_value": top_vendor_value,
            "total_value": total_value,
            "vendor_count": len(vendors),
        }

        severity = "medium"
        if top_vendor_share >= 0.95 or top_vendor_value >= 5000000:
            severity = "high"

        alert_id = create_alert(
            alert_type="exclusive_relationship",
            severity=severity,
            title=f"Agency spending concentration: {agency.name}",
            description=(
                f"{agency.name} directs {top_vendor_share:.0%} of spending "
                f"(${top_vendor_value:,.2f}) to '{top_vendor.name if top_vendor else 'Unknown'}'. "
                f"Total spending: ${total_value:,.2f} across {len(vendors)} vendors."
            ),
            entity_type="agency",
            entity_id=agency.id,
            evidence=evidence,
        )

        if alert_id:
            alerts_created += 1

    return alerts_created


def get_network_stats(session=None) -> dict:
    """Get overall network statistics."""
    if session is None:
        with get_session() as session:
            return _compute_network_stats(session)
    return _compute_network_stats(session)


def _compute_network_stats(session) -> dict:
    G = build_network_graph(session)

    vendor_nodes = [n for n in G.nodes() if n.startswith("V_")]
    agency_nodes = [n for n in G.nodes() if n.startswith("A_")]

    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "vendor_count": len(vendor_nodes),
        "agency_count": len(agency_nodes),
        "avg_vendor_degree": sum(G.degree(n) for n in vendor_nodes) / len(vendor_nodes) if vendor_nodes else 0,
        "avg_agency_degree": sum(G.degree(n) for n in agency_nodes) / len(agency_nodes) if agency_nodes else 0,
        "connected_components": nx.number_connected_components(G),
    }
