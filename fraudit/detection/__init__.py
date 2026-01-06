"""Fraud detection module for Fraudit."""

from .engine import run_detection, DetectionEngine
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

__all__ = [
    "run_detection",
    "DetectionEngine",
    "contract_splitting",
    "duplicates",
    "vendor_clustering",
    "anomalies",
    "confidentiality",
    "network",
    "crossref",
    "employee_vendor",
    "ghost_vendors",
    "fiscal_year_rush",
    "related_party",
    "debarment",
]
