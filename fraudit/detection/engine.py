"""
Parallel fraud detection engine with live progress display.

Runs all detection rules concurrently with real-time status updates.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable

from rich.console import Console
from rich.live import Live
from rich.table import Table

from fraudit.config import config


@dataclass
class DetectionTask:
    """Track state of a detection task."""
    name: str
    display_name: str
    status: str = "pending"  # pending, running, success, failed
    alerts: int = 0
    message: str = ""
    started: Optional[datetime] = None
    finished: Optional[datetime] = None
    error: Optional[str] = None


class DetectionEngine:
    """Orchestrates all fraud detection rules in parallel."""

    def __init__(self):
        self.thresholds = config.detection_thresholds
        self.alerts_created = 0
        self.console = Console()
        self.tasks: dict[str, DetectionTask] = {}
        self.lock = threading.Lock()

    def _get_rules(self) -> list[tuple[str, str, Callable]]:
        """Get all detection rules with their names and functions."""
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

        return [
            ("contract_splitting", "Contract Splitting", contract_splitting.detect),
            ("duplicates", "Duplicate Payments", duplicates.detect),
            ("vendor_clustering", "Vendor Clustering", vendor_clustering.detect),
            ("anomalies", "Payment Anomalies", anomalies.detect),
            ("confidentiality", "Confidentiality Analysis", confidentiality.detect),
            ("network", "Network Analysis", network.detect),
            ("crossref", "Cross-Reference", crossref.detect),
            ("employee_vendor", "Employee-Vendor Match", employee_vendor.detect),
            ("ghost_vendors", "Ghost Vendors", ghost_vendors.detect),
            ("fiscal_year_rush", "Fiscal Year Rush", fiscal_year_rush.detect),
            ("related_party", "Related Party", related_party.detect),
            ("debarment", "Debarment Check", debarment.detect),
        ]

    def _run_rule(self, name: str, display_name: str, detect_func: Callable) -> DetectionTask:
        """Run a single detection rule."""
        task = self.tasks[name]
        task.status = "running"
        task.started = datetime.now()
        task.message = "Analyzing..."

        try:
            alerts = detect_func(self.thresholds)
            task.alerts = alerts
            task.status = "success"
            task.message = f"{alerts} alerts" if alerts != 1 else "1 alert"
        except Exception as e:
            task.status = "failed"
            task.error = str(e)
            task.message = f"Error: {str(e)[:40]}"

        task.finished = datetime.now()
        return task

    def _make_display(self) -> Table:
        """Create the progress display table."""
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Detection Rule", style="bold", width=24)
        table.add_column("Status", width=10)
        table.add_column("Alerts", justify="right", width=8)
        table.add_column("Time", justify="right", width=8)
        table.add_column("Details", ratio=1)

        status_icons = {
            "pending": "[dim]○[/dim]",
            "running": "[yellow]◐[/yellow]",
            "success": "[green]✓[/green]",
            "failed": "[red]✗[/red]",
        }

        for name, task in self.tasks.items():
            icon = status_icons.get(task.status, "?")

            if task.status == "running":
                status = f"{icon} [yellow]running[/yellow]"
                elapsed = (datetime.now() - task.started).total_seconds() if task.started else 0
                time_str = f"{elapsed:.1f}s"
            elif task.status == "success":
                status = f"{icon} [green]done[/green]"
                if task.started and task.finished:
                    elapsed = (task.finished - task.started).total_seconds()
                    time_str = f"{elapsed:.1f}s"
                else:
                    time_str = "-"
            elif task.status == "failed":
                status = f"{icon} [red]failed[/red]"
                time_str = "-"
            else:
                status = f"{icon} [dim]pending[/dim]"
                time_str = "-"

            alerts_str = str(task.alerts) if task.alerts > 0 else "-"
            if task.alerts > 0:
                if task.alerts >= 10:
                    alerts_str = f"[yellow]{task.alerts}[/yellow]"
                else:
                    alerts_str = f"[green]{task.alerts}[/green]"

            table.add_row(task.display_name, status, alerts_str, time_str, task.message)

        return table

    def run_all(self, parallel: bool = True, max_workers: int = 6) -> int:
        """
        Run all detection rules.

        Args:
            parallel: Run rules in parallel (default True)
            max_workers: Max concurrent rules (default 6)

        Returns:
            Total alerts created
        """
        rules = self._get_rules()

        # Initialize tasks
        for name, display_name, _ in rules:
            self.tasks[name] = DetectionTask(
                name=name,
                display_name=display_name,
                status="pending",
                message="Waiting..."
            )

        if not parallel:
            # Sequential execution (for debugging)
            total = 0
            for name, display_name, detect_func in rules:
                self.tasks[name].status = "running"
                self.tasks[name].started = datetime.now()
                print(f"Running {display_name}...")
                try:
                    alerts = detect_func(self.thresholds)
                    self.tasks[name].alerts = alerts
                    self.tasks[name].status = "success"
                    total += alerts
                except Exception as e:
                    self.tasks[name].status = "failed"
                    self.tasks[name].error = str(e)
                    print(f"  Error: {e}")
                self.tasks[name].finished = datetime.now()
            self.alerts_created = total
            return total

        # Parallel execution with live display
        results = {}

        with Live(self._make_display(), console=self.console, refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._run_rule, name, display_name, detect_func): name
                    for name, display_name, detect_func in rules
                }

                while futures:
                    # Update display
                    live.update(self._make_display())

                    # Check for completed futures
                    done = [f for f in futures if f.done()]
                    for future in done:
                        name = futures.pop(future)
                        try:
                            task = future.result()
                            results[name] = task.alerts
                        except Exception as e:
                            results[name] = 0
                            self.tasks[name].status = "failed"
                            self.tasks[name].error = str(e)

                    if futures:
                        time.sleep(0.1)

                # Final update
                live.update(self._make_display())

        # Summary
        total_alerts = sum(results.values())
        success_count = sum(1 for t in self.tasks.values() if t.status == "success")
        failed_count = sum(1 for t in self.tasks.values() if t.status == "failed")

        self.console.print()
        if failed_count == 0:
            self.console.print(f"[green]✓ Detection complete:[/green] {total_alerts} alerts from {success_count} rules")
        else:
            self.console.print(f"[yellow]⚠ Detection finished:[/yellow] {total_alerts} alerts, {failed_count} rules failed")

        self.alerts_created = total_alerts
        return total_alerts

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
            f"fraudit.detection.{module_name}",
            fromlist=["detect"]
        )
        return module.detect(self.thresholds)

    def analyze_vendor(self, vendor_id: str) -> int:
        """Run all detection rules focused on a specific vendor."""
        # TODO: Implement vendor-specific analysis
        return 0


def run_detection(rule: Optional[str] = None, vendor_id: Optional[str] = None, parallel: bool = True) -> int:
    """
    Run fraud detection analysis.

    Args:
        rule: Specific rule to run, or None for all rules.
        vendor_id: Specific vendor to analyze, or None for all.
        parallel: Run rules in parallel (default True).

    Returns:
        Number of alerts created.
    """
    engine = DetectionEngine()

    if vendor_id:
        return engine.analyze_vendor(vendor_id)
    elif rule:
        return engine.run_rule(rule)
    else:
        return engine.run_all(parallel=parallel)
