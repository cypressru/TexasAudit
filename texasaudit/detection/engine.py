"""Main fraud detection engine orchestrator."""

from typing import Optional

from texasaudit.config import config


class DetectionEngine:
    """Orchestrates all fraud detection rules."""

    def __init__(self):
        self.thresholds = config.detection_thresholds
        self.alerts_created = 0

    def run_all(self) -> int:
        """Run all detection rules."""
        from . import contract_splitting
        from . import duplicates
        from . import vendor_clustering
        from . import anomalies
        from . import confidentiality
        from . import network
        from . import crossref
        from . import employee_vendor
        from . import ghost_vendors
        from . import fiscal_year_rush
        from . import related_party
        from . import debarment

        rules = [
            contract_splitting.detect,
            duplicates.detect,
            vendor_clustering.detect,
            anomalies.detect,
            confidentiality.detect,
            network.detect,
            crossref.detect,
            employee_vendor.detect,
            ghost_vendors.detect,
            fiscal_year_rush.detect,
            related_party.detect,
            debarment.detect,
        ]

        total = 0
        for rule in rules:
            total += rule(self.thresholds)

        self.alerts_created = total
        return total

    def run_rule(self, rule_name: str) -> int:
        """Run a specific detection rule."""
        rule_map = {
            "contract-splitting": "contract_splitting",
            "duplicate-payments": "duplicates",
            "vendor-clustering": "vendor_clustering",
            "payment-anomalies": "anomalies",
            "confidentiality": "confidentiality",
            "network-analysis": "network",
            "crossref": "crossref",
            "address-clusters": "crossref",
            "pay-to-play": "crossref",
            # New detection rules
            "employee-vendor": "employee_vendor",
            "ghost-vendors": "ghost_vendors",
            "fiscal-year-rush": "fiscal_year_rush",
            "related-party": "related_party",
            "debarment": "debarment",
            "sam-exclusions": "debarment",
        }

        module_name = rule_map.get(rule_name)
        if not module_name:
            raise ValueError(f"Unknown rule: {rule_name}")

        module = __import__(
            f"texasaudit.detection.{module_name}",
            fromlist=["detect"]
        )
        return module.detect(self.thresholds)

    def analyze_vendor(self, vendor_id: str) -> int:
        """Run all detection rules focused on a specific vendor."""
        # TODO: Implement vendor-specific analysis
        return 0


def run_detection(rule: Optional[str] = None, vendor_id: Optional[str] = None) -> int:
    """
    Run fraud detection analysis.

    Args:
        rule: Specific rule to run, or None for all rules.
        vendor_id: Specific vendor to analyze, or None for all.

    Returns:
        Number of alerts created.
    """
    engine = DetectionEngine()

    if vendor_id:
        return engine.analyze_vendor(vendor_id)
    elif rule:
        return engine.run_rule(rule)
    else:
        return engine.run_all()
