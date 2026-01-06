"""
Vendor Clustering Detection.

Identifies potentially related vendors that may be:
- Same entity under different names
- Shell companies
- Related parties attempting to circumvent rules

Detection methods:
- Fuzzy name matching
- Same address, different names
- Sequential vendor IDs with similar names
- Shared phone/contact info
"""

from collections import defaultdict
from decimal import Decimal

from rapidfuzz import fuzz, process
from sqlalchemy import func

from fraudit.database import get_session, Vendor, VendorRelationship
from fraudit.normalization import normalize_vendor_name, normalize_address
from fraudit.alerts import create_alert


def detect(thresholds: dict) -> int:
    """
    Run vendor clustering detection.

    Args:
        thresholds: Detection thresholds from config

    Returns:
        Number of alerts created
    """
    alerts_created = 0

    similarity_threshold = thresholds.get("vendor_name_similarity", 0.85)

    print(f"  Checking for related vendors (similarity threshold: {similarity_threshold})")

    with get_session() as session:
        # Same address detection
        print("    Checking same addresses...")
        alerts_created += _detect_same_address(session)

        # Fuzzy name matching
        print("    Checking similar names...")
        alerts_created += _detect_similar_names(session, similarity_threshold)

        # Sequential vendor IDs
        print("    Checking sequential IDs...")
        alerts_created += _detect_sequential_ids(session)

    return alerts_created


def _detect_same_address(session) -> int:
    """Find vendors sharing the same address."""
    alerts_created = 0

    # Group vendors by normalized address
    vendors = session.query(Vendor).filter(
        Vendor.address.isnot(None),
        Vendor.address != "",
    ).all()

    # Build address index
    address_groups = defaultdict(list)
    for v in vendors:
        if not v.address:
            continue
        parsed = normalize_address(v.address, v.city, v.state, v.zip_code)
        if parsed.normalized:
            address_groups[parsed.normalized].append(v)

    # Find groups with multiple vendors
    for address, vendor_list in address_groups.items():
        if len(vendor_list) < 2:
            continue

        # Skip if names are very similar (probably same vendor)
        if len(vendor_list) == 2:
            sim = fuzz.ratio(
                vendor_list[0].name_normalized or vendor_list[0].name,
                vendor_list[1].name_normalized or vendor_list[1].name,
            ) / 100.0
            if sim > 0.9:
                continue

        # Record relationships
        for i, v1 in enumerate(vendor_list):
            for v2 in vendor_list[i + 1:]:
                _record_relationship(
                    session, v1.id, v2.id,
                    "same_address",
                    confidence=0.8,
                    evidence={"address": address}
                )

        # Create alert for suspicious clusters
        if len(vendor_list) >= 3:
            # Multiple unrelated vendors at same address
            total_payments = sum(
                sum(p.amount for p in v.payments)
                for v in vendor_list
            )

            evidence = {
                "address": address,
                "vendor_count": len(vendor_list),
                "vendors": [
                    {
                        "id": v.id,
                        "name": v.name,
                        "vendor_id": v.vendor_id,
                        "payment_count": len(v.payments),
                    }
                    for v in vendor_list
                ],
                "total_payments": float(total_payments),
            }

            severity = "medium"
            if len(vendor_list) >= 5 or total_payments >= 1000000:
                severity = "high"

            alert_id = create_alert(
                alert_type="vendor_cluster_address",
                severity=severity,
                title=f"{len(vendor_list)} vendors at same address",
                description=(
                    f"Found {len(vendor_list)} different vendors registered at "
                    f"'{address}'. Combined payments: ${total_payments:,.2f}. "
                    f"This may indicate shell companies or related party transactions."
                ),
                entity_type="vendor",
                entity_id=vendor_list[0].id,
                evidence=evidence,
            )

            if alert_id:
                alerts_created += 1

    return alerts_created


def _process_vendor_batch(args):
    """Worker function for multiprocessing - finds similar vendor names."""
    batch, all_names, name_to_id, threshold = args
    batch_pairs = []

    for vid, name in batch:
        matches = process.extract(
            name, all_names,
            scorer=fuzz.ratio,
            score_cutoff=int(threshold * 100),
            limit=10,
        )

        for match_name, score, _ in matches:
            if match_name == name:
                continue

            match_id = name_to_id.get(match_name)
            if match_id and vid < match_id:  # Avoid duplicates
                batch_pairs.append((vid, match_id, score / 100.0))

    return batch_pairs


def _detect_similar_names(session, threshold: float) -> int:
    """Find vendors with suspiciously similar names using multiprocessing."""
    from concurrent.futures import ProcessPoolExecutor, as_completed
    import os

    alerts_created = 0
    cpu_count = os.cpu_count() or 4
    num_workers = max(1, cpu_count - 1)

    # Get all vendor names
    vendors = session.query(Vendor).filter(
        Vendor.name_normalized.isnot(None)
    ).all()

    vendor_names = [(v.id, v.name_normalized) for v in vendors]

    # Sample if too many (but use larger sample with multiprocessing)
    if len(vendor_names) > 30000:
        import random
        vendor_names = random.sample(vendor_names, 30000)

    print(f"      Comparing {len(vendor_names):,} vendor names ({num_workers} cores)...")

    # Build lookup structures
    name_to_id = {name: vid for vid, name in vendor_names}
    names = [name for _, name in vendor_names]

    # Split into batches for parallel processing
    batch_size = 1000
    batches = [vendor_names[i:i + batch_size] for i in range(0, len(vendor_names), batch_size)]

    # Prepare args for worker processes
    work_args = [(batch, names, name_to_id, threshold) for batch in batches]

    # Process batches in parallel using multiple CPU cores
    similar_pairs = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(_process_vendor_batch, args) for args in work_args]

        for future in as_completed(futures):
            batch_pairs = future.result()
            similar_pairs.extend(batch_pairs)

    print(f"      Found {len(similar_pairs):,} similar name pairs...")

    # Record relationships and create alerts
    for v1_id, v2_id, confidence in similar_pairs:
        v1 = session.get(Vendor, v1_id)
        v2 = session.get(Vendor, v2_id)

        if not v1 or not v2:
            continue

        _record_relationship(
            session, v1_id, v2_id,
            "similar_name",
            confidence=confidence,
            evidence={
                "name1": v1.name,
                "name2": v2.name,
                "similarity": confidence,
            }
        )

        # High confidence similar names with different addresses = suspicious
        if confidence >= 0.9:
            same_address = (
                v1.address and v2.address and
                normalize_address(v1.address).normalized == normalize_address(v2.address).normalized
            )

            if not same_address and v1.address and v2.address:
                # Different addresses but nearly identical names
                evidence = {
                    "vendor1": {"id": v1.id, "name": v1.name, "address": v1.address},
                    "vendor2": {"id": v2.id, "name": v2.name, "address": v2.address},
                    "similarity": confidence,
                }

                alert_id = create_alert(
                    alert_type="vendor_cluster_name",
                    severity="medium",
                    title=f"Nearly identical vendor names",
                    description=(
                        f"Vendors '{v1.name}' and '{v2.name}' have {confidence:.0%} "
                        f"name similarity but different addresses. "
                        f"May be the same entity or intentional duplicates."
                    ),
                    entity_type="vendor",
                    entity_id=v1_id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created


def _detect_sequential_ids(session) -> int:
    """Find sequential vendor IDs with similar names."""
    alerts_created = 0

    # Get vendors with numeric vendor IDs
    vendors = session.query(Vendor).filter(
        Vendor.vendor_id.isnot(None)
    ).order_by(Vendor.vendor_id).all()

    # Look for sequences
    for i in range(len(vendors) - 1):
        v1 = vendors[i]
        v2 = vendors[i + 1]

        # Check if IDs are sequential (assuming numeric)
        try:
            id1 = int(v1.vendor_id.replace("-", "").replace(" ", ""))
            id2 = int(v2.vendor_id.replace("-", "").replace(" ", ""))

            if id2 - id1 != 1:
                continue
        except (ValueError, AttributeError):
            continue

        # Check name similarity
        name1 = v1.name_normalized or v1.name
        name2 = v2.name_normalized or v2.name

        similarity = fuzz.ratio(name1, name2) / 100.0

        if similarity >= 0.7:
            _record_relationship(
                session, v1.id, v2.id,
                "sequential_id",
                confidence=similarity,
                evidence={
                    "vendor_id_1": v1.vendor_id,
                    "vendor_id_2": v2.vendor_id,
                    "name_similarity": similarity,
                }
            )

            # Very suspicious if high similarity
            if similarity >= 0.85:
                evidence = {
                    "vendor1": {
                        "id": v1.id,
                        "vendor_id": v1.vendor_id,
                        "name": v1.name,
                    },
                    "vendor2": {
                        "id": v2.id,
                        "vendor_id": v2.vendor_id,
                        "name": v2.name,
                    },
                    "similarity": similarity,
                }

                alert_id = create_alert(
                    alert_type="vendor_cluster_sequential",
                    severity="low",
                    title=f"Sequential vendor IDs with similar names",
                    description=(
                        f"Vendors '{v1.name}' ({v1.vendor_id}) and "
                        f"'{v2.name}' ({v2.vendor_id}) have sequential IDs "
                        f"and {similarity:.0%} name similarity. "
                        f"May indicate coordinated registration."
                    ),
                    entity_type="vendor",
                    entity_id=v1.id,
                    evidence=evidence,
                )

                if alert_id:
                    alerts_created += 1

    return alerts_created


def _record_relationship(
    session,
    vendor_id_1: int,
    vendor_id_2: int,
    relationship_type: str,
    confidence: float,
    evidence: dict,
) -> None:
    """Record a vendor relationship in the database."""
    # Ensure consistent ordering
    if vendor_id_1 > vendor_id_2:
        vendor_id_1, vendor_id_2 = vendor_id_2, vendor_id_1

    # Check if relationship exists
    existing = session.query(VendorRelationship).filter(
        VendorRelationship.vendor_id_1 == vendor_id_1,
        VendorRelationship.vendor_id_2 == vendor_id_2,
        VendorRelationship.relationship_type == relationship_type,
    ).first()

    if existing:
        # Update confidence if higher
        if confidence > (existing.confidence_score or 0):
            existing.confidence_score = Decimal(str(confidence))
            existing.evidence = evidence
    else:
        rel = VendorRelationship(
            vendor_id_1=vendor_id_1,
            vendor_id_2=vendor_id_2,
            relationship_type=relationship_type,
            confidence_score=Decimal(str(confidence)),
            evidence=evidence,
        )
        session.add(rel)


def get_vendor_cluster(vendor_id: int) -> list[dict]:
    """
    Get all vendors related to a given vendor.

    Returns:
        List of related vendor info with relationship details
    """
    with get_session() as session:
        vendor = session.get(Vendor, vendor_id)
        if not vendor:
            return []

        # Get all relationships
        relationships = session.query(VendorRelationship).filter(
            (VendorRelationship.vendor_id_1 == vendor_id) |
            (VendorRelationship.vendor_id_2 == vendor_id)
        ).all()

        related = []
        for rel in relationships:
            other_id = rel.vendor_id_2 if rel.vendor_id_1 == vendor_id else rel.vendor_id_1
            other = session.get(Vendor, other_id)

            if other:
                related.append({
                    "vendor_id": other.id,
                    "vendor_vid": other.vendor_id,
                    "name": other.name,
                    "relationship": rel.relationship_type,
                    "confidence": float(rel.confidence_score) if rel.confidence_score else None,
                    "evidence": rel.evidence,
                })

        return related
