"""
Parallel sync runner with live progress display.

Shows real-time progress for all data sources downloading in parallel.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

from fraudit.database import get_session, SyncStatus, SyncStatusEnum


@dataclass
class SyncTask:
    """Track state of a sync task."""
    name: str
    status: str = "pending"  # pending, running, success, failed, skipped
    records: int = 0
    message: str = ""
    progress: float = 0.0
    started: Optional[datetime] = None
    finished: Optional[datetime] = None
    error: Optional[str] = None


class SyncRunner:
    """Run syncs in parallel with live progress display."""

    def __init__(self, sources: list[str] = None):
        self.console = Console()
        self.sources = sources or [
            "cmbl",
            "socrata_payments",
            "lbb_contracts",
            "usaspending",
            "txsmartbuy",
            "employee_salaries",
            "tax_permits",
            "campaign_finance",
            "sam_exclusions",
            "txdot_bids",
            "txdot_contracts",
            # Note: hhs_contracts disabled - site blocks automated access
        ]
        self.tasks: dict[str, SyncTask] = {}
        self.lock = threading.Lock()

    def _get_sources_to_sync(self) -> list[str]:
        """Get list of sources that need syncing."""
        needs_sync = []

        with get_session() as s:
            for source in self.sources:
                latest = s.query(SyncStatus).filter(
                    SyncStatus.source_name == source,
                    SyncStatus.status == SyncStatusEnum.SUCCESS,
                    SyncStatus.records_synced > 0
                ).first()

                if not latest:
                    needs_sync.append(source)

        return needs_sync

    def _update_task(self, name: str, **kwargs):
        """Thread-safe task update."""
        with self.lock:
            if name in self.tasks:
                for k, v in kwargs.items():
                    setattr(self.tasks[name], k, v)

    def _run_sync(self, source: str) -> SyncTask:
        """Run a single sync and return results."""
        task = self.tasks[source]
        task.status = "running"
        task.started = datetime.now()
        task.message = "Starting..."

        try:
            if source == "cmbl":
                from fraudit.ingestion.cmbl import CMBLIngestor
                task.message = "Downloading CMBL vendor data..."
                ingestor = CMBLIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} vendors"

            elif source == "socrata_payments":
                from fraudit.ingestion.socrata import SocrataIngestor
                task.message = "Downloading expenditure data..."
                ingestor = SocrataIngestor()
                # Patch print to capture progress
                original_print = print
                def capture_print(*args, **kwargs):
                    msg = " ".join(str(a) for a in args)
                    if "FY" in msg or "record" in msg.lower():
                        task.message = msg.strip()[:50]
                    original_print(*args, **kwargs)
                import builtins
                builtins.print = capture_print
                try:
                    count = ingestor.sync()
                finally:
                    builtins.print = original_print
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} payments"

            elif source == "lbb_contracts":
                from fraudit.ingestion.lbb import LBBIngestor
                task.message = "Checking for LBB import files..."
                ingestor = LBBIngestor()
                count = ingestor.sync()
                task.records = count
                if count == 0:
                    task.message = "No import files found"
                else:
                    task.message = f"Imported {count:,} contracts"
                task.status = "success"

            elif source == "usaspending":
                from fraudit.ingestion.usaspending import USASpendingIngestor
                task.message = "Fetching federal grants & contracts..."
                ingestor = USASpendingIngestor()
                # Patch print to capture progress
                original_print = print
                def capture_print(*args, **kwargs):
                    msg = " ".join(str(a) for a in args)
                    if "FY" in msg or "Fetching" in msg or "Syncing" in msg:
                        task.message = msg.strip()[:50]
                    original_print(*args, **kwargs)
                import builtins
                builtins.print = capture_print
                try:
                    count = ingestor.sync()
                finally:
                    builtins.print = original_print
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} grants/contracts"

            elif source == "txsmartbuy":
                from fraudit.ingestion.txsmartbuy import TxSmartBuyIngestor
                task.message = "Fetching DIR contract data..."
                ingestor = TxSmartBuyIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} contracts"

            elif source == "employee_salaries":
                from fraudit.ingestion.salaries import SalariesIngestor
                task.message = "Downloading Texas Tribune salary data..."
                ingestor = SalariesIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} employees"

            elif source == "tax_permits":
                from fraudit.ingestion.taxpermits import TaxPermitsIngestor
                task.message = "Downloading tax permit data..."
                ingestor = TaxPermitsIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} tax permits"

            elif source == "comptroller_payments":
                from fraudit.ingestion.comptroller import ComptrollerPaymentsIngestor
                task.message = "Downloading Comptroller payment data..."
                ingestor = ComptrollerPaymentsIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} payments"

            elif source == "campaign_finance":
                from fraudit.ingestion.ethics import EthicsIngestor
                task.message = "Downloading TEC campaign finance data..."
                ingestor = EthicsIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} contributions"

            elif source == "sam_exclusions":
                from fraudit.ingestion.sam_exclusions import SAMExclusionsIngestor
                task.message = "Downloading SAM.gov exclusions list..."
                ingestor = SAMExclusionsIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} exclusions"

            elif source == "txdot_bids":
                from fraudit.ingestion.txdot import TxDOTBidIngestor
                task.message = "Downloading TxDOT bid tabulations..."
                ingestor = TxDOTBidIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} bids"

            elif source == "txdot_contracts":
                from fraudit.ingestion.txdot import TxDOTContractIngestor
                task.message = "Downloading TxDOT contract data..."
                ingestor = TxDOTContractIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} contracts"

            elif source == "hhs_contracts":
                from fraudit.ingestion.hhs_contracts import HHSContractsIngestor
                task.message = "Scraping HHS contracts..."
                ingestor = HHSContractsIngestor()
                count = ingestor.sync()
                task.records = count
                task.status = "success"
                task.message = f"Imported {count:,} contracts"

        except Exception as e:
            task.status = "failed"
            task.error = str(e)
            task.message = f"Error: {str(e)[:50]}"

        task.finished = datetime.now()
        return task

    def _run_socrata_parallel(self, task: SyncTask) -> int:
        """Run all socrata fiscal years in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from datetime import date
        from sodapy import Socrata
        from fraudit.config import config
        from fraudit.database import get_session, Payment, Vendor
        from fraudit.normalization import normalize_vendor_name
        from decimal import Decimal
        import threading

        # Dataset IDs by fiscal year (from data.texas.gov)
        DATASETS = {
            2024: "2zpi-yjjs",
            2023: "iyey-5sid",
            2022: "xys8-xb33",
            2021: "tup7-smjg",
            2020: "aact-g69n",
            2019: "2x5x-m677",
            2018: "f2iw-dtqt",
        }

        client = Socrata("data.texas.gov", app_token=config.socrata_token, timeout=120)
        total_new = 0
        total_scanned = 0
        year_status = {}
        status_lock = threading.Lock()

        def update_status():
            with status_lock:
                parts = [f"FY{y}: {s}" for y, s in sorted(year_status.items(), reverse=True)]
                task.message = " | ".join(parts[:4])

        def sync_year(year: int, dataset_id: str) -> tuple[int, int]:
            """Sync a single fiscal year. Returns (new_count, scanned_count)."""
            nonlocal total_scanned
            try:
                # Get count
                count_result = client.get(dataset_id, select="count(*)")
                total_count = int(count_result[0]["count"])

                with status_lock:
                    year_status[year] = f"0/{total_count//1000}k"
                update_status()

                # Get data in pages
                offset = 0
                page_size = 50000
                year_new = 0
                year_scanned = 0

                while offset < total_count:
                    records = client.get(
                        dataset_id,
                        limit=page_size,
                        offset=offset,
                        order="check_date DESC",
                    )

                    if not records:
                        break

                    year_scanned += len(records)

                    # Process batch
                    with get_session() as session:
                        # Pre-fetch existing doc_nums for this batch
                        doc_nums = [r.get("doc_num", "") for r in records if r.get("doc_num")]
                        existing_docs = set(
                            d[0] for d in session.query(Payment.document_number).filter(
                                Payment.document_number.in_(doc_nums)
                            ).all()
                        )

                        for record in records:
                            doc_num = record.get("doc_num", "")
                            if not doc_num or doc_num in existing_docs:
                                continue

                            # Parse amount
                            try:
                                amount = Decimal(str(record.get("check_amount", "0")))
                            except:
                                continue

                            # Get vendor
                            vendor_name = record.get("vendor_name", "")
                            vendor_id = None
                            if vendor_name:
                                normalized = normalize_vendor_name(vendor_name)
                                vendor = session.query(Vendor).filter(
                                    Vendor.name_normalized == normalized
                                ).first()
                                if vendor:
                                    vendor_id = vendor.id

                            # Parse date
                            payment_date = None
                            if record.get("check_date"):
                                try:
                                    from datetime import datetime
                                    payment_date = datetime.fromisoformat(
                                        record["check_date"][:10]
                                    ).date()
                                except:
                                    pass

                            payment = Payment(
                                document_number=doc_num,
                                vendor_id=vendor_id,
                                amount=amount,
                                payment_date=payment_date,
                                fiscal_year_state=year,
                                description=record.get("comptroller_obj_descr", ""),
                                source_system="socrata",
                                raw_data=record,
                            )
                            session.add(payment)
                            year_new += 1

                    offset += len(records)
                    with status_lock:
                        year_status[year] = f"{year_scanned//1000}k"
                        task.records = sum(1 for _ in year_status.values())  # Active years
                    update_status()

                with status_lock:
                    year_status[year] = f"✓{year_new}"
                update_status()
                return year_new, year_scanned

            except Exception as e:
                with status_lock:
                    year_status[year] = f"ERR"
                update_status()
                return 0, 0

        # Run all years in parallel
        task.message = f"Starting {len(DATASETS)} fiscal years..."

        with ThreadPoolExecutor(max_workers=len(DATASETS)) as executor:
            futures = {
                executor.submit(sync_year, year, dataset_id): year
                for year, dataset_id in DATASETS.items()
            }

            for future in as_completed(futures):
                year = futures[future]
                new_count, scanned = future.result()
                total_new += new_count
                total_scanned += scanned

        task.records = total_new
        task.message = f"Scanned {total_scanned:,}, added {total_new:,} new"
        return total_new

    def _run_usaspending(self, task: SyncTask) -> int:
        """Run USASpending sync with progress updates."""
        from datetime import date
        from decimal import Decimal
        import requests
        from fraudit.database import get_session, Grant, Vendor, Agency
        from fraudit.normalization import normalize_vendor_name, normalize_fiscal_years
        from fraudit.config import config

        BASE_URL = "https://api.usaspending.gov/api/v2"
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})

        # Determine fiscal years
        current_fy = normalize_fiscal_years(date.today()).federal
        start_fy = config.start_fiscal_year or 2020  # Limit to recent years

        total = 0
        total_scanned = 0

        for award_type in ["grants", "contracts"]:
            type_codes = ["02", "03", "04", "05"] if award_type == "grants" else ["A", "B", "C", "D"]

            for fy in range(start_fy, current_fy + 1):
                task.message = f"FY{fy} {award_type}..."

                filters = {
                    "place_of_performance_locations": [{"country": "USA", "state": "TX"}],
                    "time_period": [{"start_date": f"{fy - 1}-10-01", "end_date": f"{fy}-09-30"}],
                    "award_type_codes": type_codes,
                }

                page = 1
                while page < 50:  # Limit pages per FY
                    try:
                        response = session.post(
                            f"{BASE_URL}/search/spending_by_award/",
                            json={
                                "filters": filters,
                                "fields": ["Award ID", "Recipient Name", "Award Amount",
                                          "Description", "Start Date", "End Date",
                                          "Awarding Agency"],
                                "page": page,
                                "limit": 100,
                                "sort": "Award Amount",
                                "order": "desc",
                            },
                            timeout=60,
                        )

                        if not response.ok:
                            break

                        results = response.json().get("results", [])
                        if not results:
                            break

                        total_scanned += len(results)
                        task.message = f"FY{fy} {award_type}: pg{page} ({total_scanned} scanned)"

                        # Process batch
                        with get_session() as db:
                            for record in results:
                                award_id = record.get("Award ID", "")
                                if not award_id:
                                    continue

                                # Skip existing
                                if db.query(Grant).filter(Grant.federal_award_id == award_id).first():
                                    continue

                                # Get vendor
                                vendor_id = None
                                recipient_name = record.get("Recipient Name", "")
                                if recipient_name:
                                    normalized = normalize_vendor_name(recipient_name)
                                    vendor = db.query(Vendor).filter(
                                        Vendor.name_normalized == normalized
                                    ).first()
                                    if not vendor:
                                        vendor = Vendor(
                                            name=recipient_name,
                                            name_normalized=normalized,
                                            in_cmbl=False,
                                            first_seen=date.today(),
                                            last_seen=date.today(),
                                        )
                                        db.add(vendor)
                                        db.flush()
                                    vendor_id = vendor.id

                                # Get agency
                                agency_id = None
                                agency_name = record.get("Awarding Agency", "")
                                if agency_name:
                                    agency = db.query(Agency).filter(Agency.name == agency_name).first()
                                    if not agency:
                                        code = f"FED_{''.join(w[0] for w in agency_name.split()[:4]).upper()}"
                                        # Make unique
                                        base_code = code
                                        counter = 2
                                        while db.query(Agency).filter(Agency.agency_code == code).first():
                                            code = f"{base_code}_{counter}"
                                            counter += 1
                                        agency = Agency(agency_code=code, name=agency_name, category="federal")
                                        db.add(agency)
                                        db.flush()
                                    agency_id = agency.id

                                # Parse amount
                                try:
                                    amount = Decimal(str(record.get("Award Amount", 0)))
                                except:
                                    amount = None

                                grant = Grant(
                                    grant_number=award_id,
                                    recipient_id=vendor_id,
                                    agency_id=agency_id,
                                    program_name=record.get("Description", "")[:500] if record.get("Description") else None,
                                    amount_awarded=amount,
                                    fiscal_year=fy,
                                    source=f"usaspending_{award_type}",
                                    federal_award_id=award_id,
                                    raw_data=record,
                                )
                                db.add(grant)
                                total += 1

                        task.records = total

                        if len(results) < 100:
                            break
                        page += 1

                    except Exception as e:
                        task.message = f"FY{fy} {award_type}: error - {str(e)[:20]}"
                        break

        task.message = f"Scanned {total_scanned:,}, added {total:,} new"
        return total

    def _make_display(self) -> Table:
        """Create the progress display table."""
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Source", style="bold", width=20)
        table.add_column("Status", width=12)
        table.add_column("Records", justify="right", width=12)
        table.add_column("Details", ratio=1)

        status_icons = {
            "pending": "[dim]○[/dim]",
            "running": "[yellow]◐[/yellow]",
            "success": "[green]✓[/green]",
            "failed": "[red]✗[/red]",
            "skipped": "[dim]–[/dim]",
        }

        for name, task in self.tasks.items():
            icon = status_icons.get(task.status, "?")

            if task.status == "running":
                status = f"{icon} [yellow]syncing[/yellow]"
            elif task.status == "success":
                status = f"{icon} [green]done[/green]"
            elif task.status == "failed":
                status = f"{icon} [red]failed[/red]"
            elif task.status == "skipped":
                status = f"{icon} [dim]skipped[/dim]"
            else:
                status = f"{icon} [dim]pending[/dim]"

            records = f"{task.records:,}" if task.records > 0 else "-"

            table.add_row(name, status, records, task.message)

        return table

    def run(self, smart_sync: bool = True) -> dict:
        """Run all syncs with live progress display."""

        # Determine which sources to sync
        if smart_sync:
            sources_to_sync = self._get_sources_to_sync()
            if not sources_to_sync:
                self.console.print("[green]All sources already synced![/green]")
                return {}
        else:
            sources_to_sync = self.sources

        # Initialize tasks
        for source in self.sources:
            if source in sources_to_sync:
                self.tasks[source] = SyncTask(name=source, status="pending", message="Waiting...")
            else:
                self.tasks[source] = SyncTask(name=source, status="skipped", message="Already synced")

        # Run syncs with live display
        results = {}

        with Live(self._make_display(), console=self.console, refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=len(sources_to_sync)) as executor:
                futures = {
                    executor.submit(self._run_sync, source): source
                    for source in sources_to_sync
                }

                while futures:
                    # Update display
                    live.update(self._make_display())

                    # Check for completed futures
                    done = [f for f in futures if f.done()]
                    for future in done:
                        source = futures.pop(future)
                        try:
                            task = future.result()
                            results[source] = {
                                "status": task.status,
                                "records": task.records,
                                "error": task.error,
                            }
                        except Exception as e:
                            results[source] = {
                                "status": "failed",
                                "records": 0,
                                "error": str(e),
                            }

                    if futures:
                        time.sleep(0.25)

                # Final update
                live.update(self._make_display())

        # Summary
        total_records = sum(r.get("records", 0) for r in results.values())
        success_count = sum(1 for r in results.values() if r.get("status") == "success")
        failed_count = sum(1 for r in results.values() if r.get("status") == "failed")

        self.console.print()
        if failed_count == 0:
            self.console.print(f"[green]✓ Sync complete:[/green] {total_records:,} records from {success_count} sources")
        else:
            self.console.print(f"[yellow]⚠ Sync finished:[/yellow] {total_records:,} records, {failed_count} failed")

        return results


def run_sync(sources: list[str] = None, smart: bool = True) -> dict:
    """Run sync with progress display."""
    runner = SyncRunner(sources)
    return runner.run(smart_sync=smart)


if __name__ == "__main__":
    run_sync()
