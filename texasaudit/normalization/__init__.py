"""Data normalization module for Texas Audit."""

from .fiscal_year import (
    to_state_fiscal_year,
    to_federal_fiscal_year,
    normalize_fiscal_years,
)
from .vendors import normalize_vendor_name
from .addresses import normalize_address

__all__ = [
    "to_state_fiscal_year",
    "to_federal_fiscal_year",
    "normalize_fiscal_years",
    "normalize_vendor_name",
    "normalize_address",
]
