"""Fiscal year normalization utilities.

Texas state fiscal year: September 1 - August 31
Federal fiscal year: October 1 - September 30
Calendar year: January 1 - December 31
"""

from datetime import date
from typing import NamedTuple


class FiscalYears(NamedTuple):
    """Container for all fiscal year representations."""
    state: int
    federal: int
    calendar: int


def to_state_fiscal_year(d: date) -> int:
    """
    Convert a date to Texas state fiscal year.

    Texas FY runs September 1 through August 31.
    FY 2024 = Sep 1, 2023 through Aug 31, 2024
    """
    if d.month >= 9:  # September or later
        return d.year + 1
    return d.year


def to_federal_fiscal_year(d: date) -> int:
    """
    Convert a date to federal fiscal year.

    Federal FY runs October 1 through September 30.
    FY 2024 = Oct 1, 2023 through Sep 30, 2024
    """
    if d.month >= 10:  # October or later
        return d.year + 1
    return d.year


def normalize_fiscal_years(d: date) -> FiscalYears:
    """
    Get all fiscal year representations for a date.

    Returns:
        FiscalYears named tuple with state, federal, and calendar years.
    """
    return FiscalYears(
        state=to_state_fiscal_year(d),
        federal=to_federal_fiscal_year(d),
        calendar=d.year,
    )


def state_fy_start(fy: int) -> date:
    """Get the start date of a Texas state fiscal year."""
    return date(fy - 1, 9, 1)


def state_fy_end(fy: int) -> date:
    """Get the end date of a Texas state fiscal year."""
    return date(fy, 8, 31)


def federal_fy_start(fy: int) -> date:
    """Get the start date of a federal fiscal year."""
    return date(fy - 1, 10, 1)


def federal_fy_end(fy: int) -> date:
    """Get the end date of a federal fiscal year."""
    return date(fy, 9, 30)


def current_state_fy() -> int:
    """Get the current Texas state fiscal year."""
    return to_state_fiscal_year(date.today())


def current_federal_fy() -> int:
    """Get the current federal fiscal year."""
    return to_federal_fiscal_year(date.today())
