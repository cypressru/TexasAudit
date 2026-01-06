"""Data ingestion module for Fraudit."""

from .base import BaseIngestor
from .socrata import SocrataIngestor
from .cmbl import CMBLIngestor
from .lbb import LBBIngestor
from .usaspending import USASpendingIngestor
from .txsmartbuy import TxSmartBuyIngestor
from .salaries import SalariesIngestor
from .taxpermits import TaxPermitsIngestor
from .comptroller import ComptrollerPaymentsIngestor
from .ethics import EthicsIngestor
from .sam_exclusions import SAMExclusionsIngestor
from .txdot import TxDOTBidIngestor, TxDOTContractIngestor
from .hhs_contracts import HHSContractsIngestor


def run_sync(sources: list[str] | None = None, full: bool = False) -> dict:
    """
    Run data synchronization for specified sources.

    Args:
        sources: List of source names to sync. If None, sync all.
        full: If True, ignore last sync timestamp and do full sync.

    Returns:
        Dict with sync results per source.
    """
    from fraudit.config import config

    all_sources = {
        "cmbl": CMBLIngestor,
        "socrata_payments": SocrataIngestor,
        "lbb_contracts": LBBIngestor,
        "usaspending": USASpendingIngestor,
        "txsmartbuy": TxSmartBuyIngestor,
        "employee_salaries": SalariesIngestor,
        "tax_permits": TaxPermitsIngestor,
        "comptroller_payments": ComptrollerPaymentsIngestor,
        "campaign_finance": EthicsIngestor,
        "sam_exclusions": SAMExclusionsIngestor,
        "txdot_bids": TxDOTBidIngestor,
        "txdot_contracts": TxDOTContractIngestor,
        "hhs_contracts": HHSContractsIngestor,
    }

    if sources is None:
        sources = config.sync_sources

    results = {}
    for source in sources:
        if source not in all_sources:
            results[source] = {"status": "error", "message": f"Unknown source: {source}"}
            continue

        ingestor = all_sources[source]()
        try:
            count = ingestor.sync(full=full)
            results[source] = {"status": "success", "records": count}
        except Exception as e:
            results[source] = {"status": "error", "message": str(e)}

    return results


__all__ = [
    "BaseIngestor",
    "SocrataIngestor",
    "CMBLIngestor",
    "LBBIngestor",
    "USASpendingIngestor",
    "TxSmartBuyIngestor",
    "SalariesIngestor",
    "TaxPermitsIngestor",
    "ComptrollerPaymentsIngestor",
    "EthicsIngestor",
    "SAMExclusionsIngestor",
    "TxDOTBidIngestor",
    "TxDOTContractIngestor",
    "HHSContractsIngestor",
    "run_sync",
]
