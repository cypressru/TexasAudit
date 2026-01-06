"""Main TUI Application for Fraudit."""

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, Grid, ScrollableContainer, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Header, Footer, Static, DataTable, Label, Button,
    TabbedContent, TabPane, Input, Rule, Sparkline,
)
from textual.reactive import reactive
from textual import work
from rich.text import Text
from rich.table import Table
from rich.panel import Panel

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from collections import defaultdict

from fraudit.database import (
    get_session, Payment, Vendor, Contract, Agency, Alert, Grant,
    SyncStatus, AlertSeverity, SyncStatusEnum,
)
# Try to import extended models (may not exist yet)
try:
    from fraudit.database import Employee, CampaignContribution, TaxPermit, EntityMatch, DebarredEntity
    HAS_EXTENDED_MODELS = True
except ImportError:
    HAS_EXTENDED_MODELS = False
    Employee = None
    CampaignContribution = None
    TaxPermit = None
    EntityMatch = None
    DebarredEntity = None
from sqlalchemy import func, desc, and_

# HUB status code mappings
HUB_ETHNICITY_MAP = {
    "X": "Multiple Certs", "N": "Non-HUB", "I": "American Indian",
    "A": "Asian Pacific", "R": "Black American", "D": "Disabled Vet",
    "M": "Hispanic", "V": "Veteran", "G": "Service Disabled Vet",
    "Non HUB": "Non-HUB", "N/A": "Non-HUB", "Woman Owned": "Woman Owned",
    "Disabled Veteran": "Disabled Vet", "Asian/Male": "Asian Pacific",
    "Asian/Female": "Asian Pacific - Woman", "Hispanic/Male": "Hispanic",
    "Hispanic/Female": "Hispanic - Woman", "Black/Male": "Black American",
    "Black/Female": "Black American - Woman", "Native American/Male": "American Indian",
    "Native American/Female": "American Indian - Woman",
}

def normalize_hub_status(status):
    """Normalize HUB status to a standard category."""
    if not status:
        return "Unknown"
    status = status.strip()
    if status in HUB_ETHNICITY_MAP:
        return HUB_ETHNICITY_MAP[status]
    status_lower = status.lower()
    if "woman" in status_lower or "female" in status_lower:
        if "asian" in status_lower: return "Asian Pacific - Woman"
        elif "hispanic" in status_lower: return "Hispanic - Woman"
        elif "black" in status_lower: return "Black American - Woman"
        return "Woman Owned"
    if "asian" in status_lower: return "Asian Pacific"
    if "hispanic" in status_lower: return "Hispanic"
    if "black" in status_lower: return "Black American"
    if "veteran" in status_lower: return "Disabled Vet"
    if "non" in status_lower or status == "N": return "Non-HUB"
    return status


def create_ascii_pie_chart(data: list, title: str = "", width: int = 70) -> str:
    """Create an ASCII pie chart representation.

    Args:
        data: List of (label, value) tuples
        title: Chart title
        width: Width of the chart in characters

    Returns:
        ASCII string representation of pie chart
    """
    if not data:
        return "No data"

    total = sum(v for _, v in data)
    if total == 0:
        return "No data"

    # Colors for Rich markup
    colors = ["cyan", "green", "yellow", "magenta", "red", "blue", "white", "bright_cyan", "bright_green", "bright_yellow"]

    lines = []
    if title:
        lines.append(f"[bold]{title}[/bold]")
        lines.append("")

    # Create bar representation for each segment
    bar_width = width - 35  # Leave room for labels and percentages

    for i, (label, value) in enumerate(data):
        pct = (value / total) * 100
        bar_len = int((value / total) * bar_width)
        color = colors[i % len(colors)]

        # Create the bar
        bar = f"[{color}]{'█' * bar_len}[/{color}]" + f"[dim]{'░' * (bar_width - bar_len)}[/dim]"

        # Format the line
        label_str = label[:18].ljust(18)
        value_str = f"{value:,}".rjust(8)
        pct_str = f"({pct:.1f}%)".rjust(8)

        lines.append(f"{label_str} {bar} {value_str} {pct_str}")

    # Add total
    lines.append("")
    lines.append(f"[bold]{'Total':18} {' ' * bar_width} {total:>8,}[/bold]")

    return "\n".join(lines)


def create_ascii_bar_chart(data: list, title: str = "", horizontal: bool = True, value_suffix: str = "") -> str:
    """Create a clean ASCII bar chart using Rich markup.

    Args:
        data: List of (label, value) tuples
        title: Chart title
        horizontal: True for horizontal bars, False for vertical
        value_suffix: Suffix to append to values (e.g., "M", "B", "%")

    Returns:
        ASCII string representation of bar chart
    """
    if not data:
        return "No data"

    max_value = max(v for _, v in data)
    if max_value == 0:
        return "No data"

    # Colors for Rich markup
    colors = ["cyan", "green", "yellow", "magenta", "red", "blue", "bright_cyan", "bright_green", "bright_yellow", "bright_magenta"]

    lines = []
    if title:
        lines.append(f"[bold]{title}[/bold]")
        lines.append("")

    if horizontal:
        # Horizontal bar chart
        bar_width = 50  # Maximum bar width
        label_width = max(len(label) for label, _ in data)
        label_width = min(label_width, 25)  # Cap at 25 chars

        for i, (label, value) in enumerate(data):
            bar_len = int((value / max_value) * bar_width) if max_value > 0 else 0
            color = colors[i % len(colors)]

            # Create the bar
            bar = f"[{color}]{'█' * bar_len}[/{color}]"

            # Format the value
            if value_suffix:
                value_str = f"{value:.1f}{value_suffix}"
            else:
                value_str = f"{value:,.0f}"

            # Format the line
            label_str = label[:label_width].ljust(label_width)
            lines.append(f"{label_str} {bar} [green]{value_str}[/green]")
    else:
        # Vertical bar chart (simple implementation)
        bar_height = 10  # Maximum bar height in characters
        label_width = max(len(label) for label, _ in data)
        label_width = min(label_width, 15)  # Cap at 15 chars

        # Build from top to bottom
        for row in range(bar_height, -1, -1):
            line_parts = []
            for i, (label, value) in enumerate(data):
                bar_height_actual = int((value / max_value) * bar_height) if max_value > 0 else 0
                color = colors[i % len(colors)]

                if row <= bar_height_actual:
                    line_parts.append(f"[{color}]███[/{color}]")
                elif row == bar_height_actual + 1:
                    # Show value on top of bar
                    if value_suffix:
                        val_str = f"{value:.0f}{value_suffix}"
                    else:
                        val_str = f"{value:.0f}"
                    line_parts.append(f"{val_str:>3}"[:3])
                else:
                    line_parts.append("   ")

            lines.append("  ".join(line_parts))

        # Add labels at bottom
        label_line = "  ".join([label[:15].ljust(15) for label, _ in data])
        lines.append("─" * len(label_line))
        lines.append(label_line)

    return "\n".join(lines)


class StatCard(Static):
    """A styled stat card."""

    def __init__(self, title: str, value: str = "0", icon: str = "", **kwargs):
        super().__init__(**kwargs)
        self.card_title = title
        self.card_value = value
        self.icon = icon

    def compose(self) -> ComposeResult:
        yield Static(self.icon, classes="stat-icon")
        yield Static(self.card_title, classes="stat-title")
        yield Static(self.card_value, classes="stat-value", id=f"stat-{self.id}")


class StatsPanel(Static):
    """Panel showing database statistics."""

    def compose(self) -> ComposeResult:
        yield Static("[b]DATABASE OVERVIEW[/b]", classes="panel-title")
        yield Horizontal(
            StatCard("Payments", "0", "", id="payments"),
            StatCard("Vendors", "0", "", id="vendors"),
            StatCard("Contracts", "0", "", id="contracts"),
            StatCard("Agencies", "0", "", id="agencies"),
            classes="stats-row",
        )
        yield Static("", id="total-spending", classes="total-spending")

    def on_mount(self) -> None:
        self.refresh_stats()

    def refresh_stats(self) -> None:
        with get_session() as s:
            payments = s.query(Payment).count()
            vendors = s.query(Vendor).count()
            contracts = s.query(Contract).count()
            agencies = s.query(Agency).count()
            total = s.query(func.sum(Payment.amount)).scalar() or 0

        self.query_one("#stat-payments", Static).update(f"{payments:,}")
        self.query_one("#stat-vendors", Static).update(f"{vendors:,}")
        self.query_one("#stat-contracts", Static).update(f"{contracts:,}")
        self.query_one("#stat-agencies", Static).update(f"{agencies:,}")
        self.query_one("#total-spending", Static).update(
            f"[b]Total Tracked:[/b] [green]${total:,.0f}[/green]"
        )


class SyncPanel(Static):
    """Panel showing sync status."""

    def compose(self) -> ComposeResult:
        yield Static("[b]SYNC STATUS[/b]", classes="panel-title")
        yield Static("", id="sync-list", classes="sync-list")

    def on_mount(self) -> None:
        self.refresh_sync()

    def refresh_sync(self) -> None:
        with get_session() as s:
            # All configured sync sources
            sources = [
                "cmbl", "socrata_payments", "lbb_contracts", "usaspending",
                "txsmartbuy", "sam_exclusions", "employee_salaries",
                "campaign_finance", "tax_permits", "txdot_bids",
                "txdot_contracts"
            ]
            lines = []

            for source in sources:
                latest = s.query(SyncStatus).filter(
                    SyncStatus.source_name == source
                ).order_by(SyncStatus.started_at.desc()).first()

                if latest:
                    if latest.status == SyncStatusEnum.SUCCESS:
                        icon = "[green]●[/green]"
                        status = "[green]ok[/green]"
                    elif latest.status == SyncStatusEnum.FAILED:
                        icon = "[red]●[/red]"
                        status = "[red]fail[/red]"
                    else:
                        icon = "[yellow]●[/yellow]"
                        status = "[yellow]...[/yellow]"

                    records = f"{latest.records_synced:,}" if latest.records_synced else "0"
                    lines.append(f"{icon} {source:<18} {records:>8} {status}")
                else:
                    lines.append(f"[dim]○ {source:<18} --[/dim]")

        self.query_one("#sync-list", Static).update("\n".join(lines))


class AlertsPanel(Static):
    """Panel showing recent alerts."""

    def compose(self) -> ComposeResult:
        yield Static("[b]ALERTS[/b]", classes="panel-title")
        yield Horizontal(
            Static("", id="alert-high", classes="alert-badge alert-high"),
            Static("", id="alert-med", classes="alert-badge alert-med"),
            Static("", id="alert-low", classes="alert-badge alert-low"),
            classes="alert-badges",
        )
        yield Static("", id="alert-list", classes="alert-list")

    def on_mount(self) -> None:
        self.refresh_alerts()

    def refresh_alerts(self) -> None:
        with get_session() as s:
            high = s.query(Alert).filter(Alert.severity == AlertSeverity.HIGH).count()
            med = s.query(Alert).filter(Alert.severity == AlertSeverity.MEDIUM).count()
            low = s.query(Alert).filter(Alert.severity == AlertSeverity.LOW).count()

            self.query_one("#alert-high", Static).update(f"[red bold]{high}[/] HIGH")
            self.query_one("#alert-med", Static).update(f"[yellow]{med}[/] MED")
            self.query_one("#alert-low", Static).update(f"[dim]{low}[/] LOW")

            alerts = s.query(Alert).order_by(
                Alert.severity.desc(),
                Alert.created_at.desc()
            ).limit(8).all()

            lines = []
            for alert in alerts:
                if alert.severity == AlertSeverity.HIGH:
                    sev = "[red]▐[/red]"
                elif alert.severity == AlertSeverity.MEDIUM:
                    sev = "[yellow]▐[/yellow]"
                else:
                    sev = "[dim]▐[/dim]"

                title = (alert.title or "Untitled")[:42]
                lines.append(f"{sev} {title}")

            if not lines:
                lines.append("[dim]No alerts[/dim]")

        self.query_one("#alert-list", Static).update("\n".join(lines))


class TopVendorsPanel(Static):
    """Panel showing top vendors."""

    def compose(self) -> ComposeResult:
        yield Static("[b]TOP VENDORS BY SPEND[/b]", classes="panel-title")
        yield Static("", id="vendor-list", classes="vendor-list")

    def on_mount(self) -> None:
        self.refresh_vendors()

    def refresh_vendors(self) -> None:
        with get_session() as s:
            results = s.query(
                Vendor.name,
                func.sum(Payment.amount).label("total")
            ).join(Payment).group_by(Vendor.id).order_by(
                desc("total")
            ).limit(8).all()

            lines = []
            if results:
                max_total = float(results[0][1]) if results[0][1] else 1
                for name, total in results:
                    name_short = (name or "Unknown")[:30]
                    total_f = float(total) if total else 0
                    bar_len = int((total_f / max_total) * 15)
                    bar = "[cyan]" + "█" * bar_len + "[/cyan]" + "[dim]" + "░" * (15 - bar_len) + "[/dim]"
                    lines.append(f"{name_short:<30} {bar} [green]${total_f:>12,.0f}[/green]")
            else:
                lines.append("[dim]No payment data[/dim]")

        self.query_one("#vendor-list", Static).update("\n".join(lines))


class DashboardScreen(Container):
    """Main dashboard view."""

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                StatsPanel(classes="panel"),
                SyncPanel(classes="panel"),
                id="left-col",
            ),
            Vertical(
                AlertsPanel(classes="panel"),
                TopVendorsPanel(classes="panel"),
                id="right-col",
            ),
            id="dashboard-grid",
        )


class VendorsScreen(Container):
    """Vendors list view with pagination."""

    BINDINGS = [
        Binding("pageup", "prev_page", "Previous Page"),
        Binding("pagedown", "next_page", "Next Page"),
        Binding("[", "prev_page", "Previous Page"),
        Binding("]", "next_page", "Next Page"),
    ]

    PAGE_SIZE = 50

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_page = 0
        self.total_vendors = 0
        self.current_search = None
        self.current_high_risk = False

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Input(placeholder="Search vendors...", id="vendor-search"),
            Button("Search", id="vendor-search-btn", variant="primary"),
            Button("High Risk", id="vendor-risk-btn", variant="warning"),
            Button("◀ Prev", id="vendor-prev-btn", variant="primary"),
            Button("Next ▶", id="vendor-next-btn", variant="primary"),
            Button("Refresh", id="vendor-refresh-btn", variant="default"),
            classes="controls",
        )
        yield Static("", id="vendors-pagination-info")
        yield DataTable(id="vendors-table")

    def on_mount(self) -> None:
        table = self.query_one("#vendors-table", DataTable)
        table.add_columns("ID", "Name", "HUB Status", "CMBL", "Risk", "Total Payments")
        table.cursor_type = "row"
        table.zebra_stripes = True
        self.load_vendors()

    def load_vendors(self, search: str = None, high_risk: bool = False, reset_page: bool = True) -> None:
        if reset_page:
            self.current_page = 0
        self.current_search = search
        self.current_high_risk = high_risk

        table = self.query_one("#vendors-table", DataTable)
        table.clear()

        with get_session() as s:
            # Count query (without aggregation for accurate count)
            count_query = s.query(func.count(Vendor.id))
            if search:
                count_query = count_query.filter(Vendor.name.ilike(f"%{search}%"))
            if high_risk:
                count_query = count_query.filter(Vendor.risk_score >= 50)
            self.total_vendors = count_query.scalar() or 0

            total_pages = max(1, (self.total_vendors + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
            start_idx = self.current_page * self.PAGE_SIZE + 1
            end_idx = min((self.current_page + 1) * self.PAGE_SIZE, self.total_vendors)
            page_info = f"[cyan]Page {self.current_page + 1}/{total_pages}[/cyan] | Showing {start_idx}-{end_idx} of {self.total_vendors:,} vendors | [dim]PageUp/PageDown or [ ] to navigate[/dim]"
            self.query_one("#vendors-pagination-info", Static).update(page_info)

            # Data query
            query = s.query(
                Vendor.id,
                Vendor.name,
                Vendor.hub_status,
                Vendor.in_cmbl,
                Vendor.risk_score,
                func.sum(Payment.amount).label("total")
            ).outerjoin(Payment).group_by(Vendor.id)

            if search:
                query = query.filter(Vendor.name.ilike(f"%{search}%"))
            if high_risk:
                query = query.filter(Vendor.risk_score >= 50)

            offset = self.current_page * self.PAGE_SIZE
            query = query.order_by(desc("total")).offset(offset).limit(self.PAGE_SIZE)

            for vid, name, hub, cmbl, risk, total in query.all():
                hub_str = (hub or "-")[:12]
                cmbl_str = "Yes" if cmbl else "No"
                risk_str = str(risk) if risk else "-"
                total_str = f"${total:,.0f}" if total else "$0"

                table.add_row(
                    str(vid),
                    (name or "Unknown")[:35],
                    hub_str,
                    cmbl_str,
                    risk_str,
                    total_str,
                    key=str(vid),
                )

    def action_prev_page(self) -> None:
        if self.current_page > 0:
            self.current_page -= 1
            self.load_vendors(search=self.current_search, high_risk=self.current_high_risk, reset_page=False)

    def action_next_page(self) -> None:
        total_pages = max(1, (self.total_vendors + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_vendors(search=self.current_search, high_risk=self.current_high_risk, reset_page=False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "vendor-search-btn":
            search_input = self.query_one("#vendor-search", Input)
            self.load_vendors(search=search_input.value)
        elif event.button.id == "vendor-risk-btn":
            self.load_vendors(high_risk=True)
        elif event.button.id == "vendor-prev-btn":
            self.action_prev_page()
        elif event.button.id == "vendor-next-btn":
            self.action_next_page()
        elif event.button.id == "vendor-refresh-btn":
            self.load_vendors()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "vendor-search":
            self.load_vendors(search=event.value)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle double-click on vendor row to show details."""
        if event.row_key:
            vendor_id = int(event.row_key.value)
            self.app.push_screen(VendorDetailModal(vendor_id))



class PaymentsScreen(Container):
    """Payments list view with pagination."""

    BINDINGS = [
        Binding("pageup", "prev_page", "Previous Page"),
        Binding("pagedown", "next_page", "Next Page"),
        Binding("[", "prev_page", "Previous Page"),
        Binding("]", "next_page", "Next Page"),
    ]

    PAGE_SIZE = 50

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_page = 0
        self.total_payments = 0
        self.current_min = None
        self.current_max = None

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Input(placeholder="Min $", id="payment-min"),
            Input(placeholder="Max $", id="payment-max"),
            Button("Filter", id="payment-filter-btn", variant="primary"),
            Button("$100K+", id="payment-large-btn", variant="warning"),
            Button("◀ Prev", id="payment-prev-btn", variant="primary"),
            Button("Next ▶", id="payment-next-btn", variant="primary"),
            Button("Refresh", id="payment-refresh-btn", variant="default"),
            classes="controls",
        )
        yield Static("", id="payments-pagination-info")
        yield DataTable(id="payments-table")

    def on_mount(self) -> None:
        table = self.query_one("#payments-table", DataTable)
        table.add_columns("Date", "Vendor", "Agency", "Amount", "Description")
        table.cursor_type = "row"
        table.zebra_stripes = True
        self.load_payments()

    def load_payments(self, min_amount: float = None, max_amount: float = None, reset_page: bool = True) -> None:
        if reset_page:
            self.current_page = 0
        self.current_min = min_amount
        self.current_max = max_amount

        table = self.query_one("#payments-table", DataTable)
        table.clear()

        with get_session() as s:
            # Count query
            count_query = s.query(func.count(Payment.id))
            if min_amount:
                count_query = count_query.filter(Payment.amount >= min_amount)
            if max_amount:
                count_query = count_query.filter(Payment.amount <= max_amount)
            self.total_payments = count_query.scalar() or 0

            total_pages = max(1, (self.total_payments + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
            start_idx = self.current_page * self.PAGE_SIZE + 1
            end_idx = min((self.current_page + 1) * self.PAGE_SIZE, self.total_payments)
            page_info = f"[cyan]Page {self.current_page + 1}/{total_pages}[/cyan] | Showing {start_idx}-{end_idx} of {self.total_payments:,} payments | [dim]PageUp/PageDown or [ ] to navigate[/dim]"
            self.query_one("#payments-pagination-info", Static).update(page_info)

            # Data query
            query = s.query(Payment)
            if min_amount:
                query = query.filter(Payment.amount >= min_amount)
            if max_amount:
                query = query.filter(Payment.amount <= max_amount)

            offset = self.current_page * self.PAGE_SIZE
            query = query.order_by(Payment.amount.desc()).offset(offset).limit(self.PAGE_SIZE)

            for p in query.all():
                date_str = p.payment_date.strftime("%Y-%m-%d") if p.payment_date else "-"
                vendor_name = (p.vendor.name if p.vendor else "Unknown")[:25]
                agency_name = (p.agency.name if p.agency else "-")[:20]
                amount_str = f"${p.amount:,.2f}"
                desc = (p.description or "-")[:30]

                table.add_row(
                    date_str, vendor_name, agency_name, amount_str, desc,
                    key=str(p.id),
                )

    def action_prev_page(self) -> None:
        if self.current_page > 0:
            self.current_page -= 1
            self.load_payments(min_amount=self.current_min, max_amount=self.current_max, reset_page=False)

    def action_next_page(self) -> None:
        total_pages = max(1, (self.total_payments + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_payments(min_amount=self.current_min, max_amount=self.current_max, reset_page=False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "payment-filter-btn":
            min_input = self.query_one("#payment-min", Input)
            max_input = self.query_one("#payment-max", Input)
            min_val = float(min_input.value) if min_input.value else None
            max_val = float(max_input.value) if max_input.value else None
            self.load_payments(min_amount=min_val, max_amount=max_val)
        elif event.button.id == "payment-large-btn":
            self.load_payments(min_amount=100000)
        elif event.button.id == "payment-prev-btn":
            self.action_prev_page()
        elif event.button.id == "payment-next-btn":
            self.action_next_page()
        elif event.button.id == "payment-refresh-btn":
            self.load_payments()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle double-click on payment row to show details."""
        if event.row_key:
            payment_id = int(event.row_key.value)
            self.app.push_screen(PaymentDetailModal(payment_id))



class ContractsScreen(Container):
    """Contracts list view with pagination."""

    BINDINGS = [
        Binding("pageup", "prev_page", "Previous Page"),
        Binding("pagedown", "next_page", "Next Page"),
        Binding("[", "prev_page", "Previous Page"),
        Binding("]", "next_page", "Next Page"),
    ]

    PAGE_SIZE = 50

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_page = 0
        self.total_contracts = 0
        self.current_search = None
        self.current_expiring = False

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Input(placeholder="Search contracts...", id="contract-search"),
            Button("Search", id="contract-search-btn", variant="primary"),
            Button("Expiring", id="contract-expiring-btn", variant="warning"),
            Button("◀ Prev", id="contract-prev-btn", variant="primary"),
            Button("Next ▶", id="contract-next-btn", variant="primary"),
            Button("Refresh", id="contract-refresh-btn", variant="default"),
            classes="controls",
        )
        yield Static("", id="contracts-pagination-info")
        yield DataTable(id="contracts-table")

    def on_mount(self) -> None:
        table = self.query_one("#contracts-table", DataTable)
        table.add_columns("Contract #", "Vendor", "Value", "Start", "End", "Source")
        table.cursor_type = "row"
        table.zebra_stripes = True
        self.load_contracts()

    def load_contracts(self, search: str = None, expiring: bool = False, reset_page: bool = True) -> None:
        if reset_page:
            self.current_page = 0
        self.current_search = search
        self.current_expiring = expiring

        table = self.query_one("#contracts-table", DataTable)
        table.clear()

        with get_session() as s:
            # Count query
            count_query = s.query(func.count(Contract.id))
            if search:
                count_query = count_query.filter(
                    Contract.contract_number.ilike(f"%{search}%") |
                    Contract.description.ilike(f"%{search}%")
                )
            if expiring:
                from datetime import date, timedelta
                soon = date.today() + timedelta(days=90)
                count_query = count_query.filter(
                    Contract.end_date <= soon,
                    Contract.end_date >= date.today()
                )
            self.total_contracts = count_query.scalar() or 0

            total_pages = max(1, (self.total_contracts + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
            start_idx = self.current_page * self.PAGE_SIZE + 1
            end_idx = min((self.current_page + 1) * self.PAGE_SIZE, self.total_contracts)
            page_info = f"[cyan]Page {self.current_page + 1}/{total_pages}[/cyan] | Showing {start_idx}-{end_idx} of {self.total_contracts:,} contracts | [dim]PageUp/PageDown or [ ] to navigate[/dim]"
            self.query_one("#contracts-pagination-info", Static).update(page_info)

            # Data query
            query = s.query(Contract)
            if search:
                query = query.filter(
                    Contract.contract_number.ilike(f"%{search}%") |
                    Contract.description.ilike(f"%{search}%")
                )
            if expiring:
                from datetime import date, timedelta
                soon = date.today() + timedelta(days=90)
                query = query.filter(
                    Contract.end_date <= soon,
                    Contract.end_date >= date.today()
                )

            offset = self.current_page * self.PAGE_SIZE
            query = query.order_by(Contract.current_value.desc().nullslast()).offset(offset).limit(self.PAGE_SIZE)

            for c in query.all():
                vendor_name = (c.vendor.name if c.vendor else "-")[:25]
                value_str = f"${c.current_value:,.0f}" if c.current_value else "-"
                start = c.start_date.strftime("%Y-%m-%d") if c.start_date else "-"
                end = c.end_date.strftime("%Y-%m-%d") if c.end_date else "-"

                table.add_row(
                    (c.contract_number or "-")[:15],
                    vendor_name,
                    value_str,
                    start,
                    end,
                    c.source or "-",
                    key=str(c.id),
                )

    def action_prev_page(self) -> None:
        if self.current_page > 0:
            self.current_page -= 1
            self.load_contracts(search=self.current_search, expiring=self.current_expiring, reset_page=False)

    def action_next_page(self) -> None:
        total_pages = max(1, (self.total_contracts + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_contracts(search=self.current_search, expiring=self.current_expiring, reset_page=False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "contract-search-btn":
            search_input = self.query_one("#contract-search", Input)
            self.load_contracts(search=search_input.value)
        elif event.button.id == "contract-expiring-btn":
            self.load_contracts(expiring=True)
        elif event.button.id == "contract-prev-btn":
            self.action_prev_page()
        elif event.button.id == "contract-next-btn":
            self.action_next_page()
        elif event.button.id == "contract-refresh-btn":
            self.load_contracts()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "contract-search":
            self.load_contracts(search=event.value)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle double-click on contract row to show details."""
        if event.row_key:
            contract_id = int(event.row_key.value)
            self.app.push_screen(ContractDetailModal(contract_id))



class AlertDetailModal(ModalScreen):
    """Modal to show detailed alert information with evidence."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    AlertDetailModal {
        align: center middle;
    }

    #alert-detail-container {
        width: 90%;
        height: 85%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #alert-detail-title {
        text-style: bold;
        text-align: center;
        padding: 1;
        border-bottom: solid $primary;
    }

    #alert-detail-content {
        height: auto;
        padding: 1;
    }

    #alert-scroll {
        height: 1fr;
    }

    .evidence-section {
        margin: 1 0;
        padding: 1;
        border: round $secondary;
    }

    .evidence-title {
        text-style: bold;
        color: $warning;
    }

    .vendor-item {
        padding: 0 0 0 2;
    }

    .close-hint {
        text-align: center;
        color: $text-muted;
        dock: bottom;
        padding: 1;
    }
    """

    def __init__(self, alert_id: int, **kwargs):
        super().__init__(**kwargs)
        self.alert_id = alert_id

    def compose(self) -> ComposeResult:
        with Container(id="alert-detail-container"):
            yield Static("", id="alert-detail-title")
            yield VerticalScroll(Static("", id="alert-detail-content"), id="alert-scroll")
            yield Static("[dim]Press ESC or Q to close[/dim]", classes="close-hint")

    def on_mount(self) -> None:
        self.load_alert_details()

    def load_alert_details(self) -> None:
        """Load and display alert details."""
        with get_session() as s:
            alert = s.query(Alert).filter(Alert.id == self.alert_id).first()
            if not alert:
                self.query_one("#alert-detail-title", Static).update("[red]Alert not found[/red]")
                return

            # Title
            severity_color = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "blue"}.get(
                alert.severity.name if alert.severity else "", "white"
            )
            title = f"[{severity_color} bold]{alert.severity.name if alert.severity else 'UNKNOWN'}[/{severity_color} bold] - {alert.title}"
            self.query_one("#alert-detail-title", Static).update(title)

            # Build content based on alert type
            content = self._format_alert_content(alert, s)
            self.query_one("#alert-detail-content", Static).update(content)

    def _format_alert_content(self, alert: Alert, session) -> str:
        """Format alert content based on type."""
        lines = []

        # Basic info
        lines.append(f"[bold]Alert Type:[/bold] {alert.alert_type}")
        lines.append(f"[bold]Created:[/bold] {alert.created_at.strftime('%Y-%m-%d %H:%M') if alert.created_at else 'Unknown'}")
        lines.append(f"[bold]Status:[/bold] {alert.status.value if alert.status else 'Unknown'}")
        if alert.description:
            lines.append(f"[bold]Description:[/bold] {alert.description}")
        lines.append("")

        evidence = alert.evidence or {}

        if alert.alert_type == "vendor_cluster_address":
            lines.extend(self._format_address_cluster(evidence, session))
        elif alert.alert_type == "vendor_cluster_name":
            lines.extend(self._format_name_cluster(evidence, session))
        elif alert.alert_type == "vendor_cluster_sequential":
            lines.extend(self._format_sequential_cluster(evidence, session))
        elif alert.alert_type == "contract_splitting":
            lines.extend(self._format_contract_splitting(evidence, session))
        else:
            # Generic evidence display
            lines.append("[bold cyan]Evidence:[/bold cyan]")
            for key, value in evidence.items():
                lines.append(f"  [green]{key}:[/green] {value}")

        return "\n".join(lines)

    def _format_address_cluster(self, evidence: dict, session) -> list:
        """Format address cluster alert evidence."""
        lines = []
        lines.append("[bold cyan]═══ VENDORS AT SAME ADDRESS ═══[/bold cyan]")
        lines.append("")

        address = evidence.get("address", "Unknown address")
        vendor_count = evidence.get("vendor_count", 0)
        total_payments = evidence.get("total_payments", 0)

        lines.append(f"[bold]Address:[/bold] [yellow]{address}[/yellow]")
        lines.append(f"[bold]Number of vendors:[/bold] {vendor_count}")
        lines.append(f"[bold]Total payments to these vendors:[/bold] ${total_payments:,.2f}")
        lines.append("")

        lines.append("[bold green]Vendors at this address:[/bold green]")
        lines.append("─" * 60)

        vendors = evidence.get("vendors", [])
        for v in vendors:
            name = v.get("name", "Unknown")
            vid = v.get("vendor_id", "N/A")
            payment_count = v.get("payment_count", 0)

            # Get source info from database
            vendor_obj = session.query(Vendor).filter(Vendor.id == v.get("id")).first()
            source = "CMBL" if vendor_obj and vendor_obj.in_cmbl else "LBB/Other"

            lines.append(f"  [cyan]●[/cyan] [bold]{name}[/bold]")
            lines.append(f"    Vendor ID: {vid}")
            lines.append(f"    Payments: {payment_count}")
            lines.append(f"    Source: [magenta]{source}[/magenta]")
            lines.append("")

        lines.append("[bold red]⚠ Red Flag:[/bold red] Multiple businesses at the same address")
        lines.append("  may indicate shell companies or related party transactions.")

        return lines

    def _format_name_cluster(self, evidence: dict, session) -> list:
        """Format name similarity alert evidence."""
        lines = []
        lines.append("[bold cyan]═══ NEARLY IDENTICAL VENDOR NAMES ═══[/bold cyan]")
        lines.append("")

        similarity = evidence.get("similarity", 0) * 100
        lines.append(f"[bold]Name Similarity:[/bold] [yellow]{similarity:.1f}%[/yellow]")
        lines.append("")

        v1 = evidence.get("vendor1", {})
        v2 = evidence.get("vendor2", {})

        lines.append("[bold green]Vendor 1:[/bold green]")
        lines.append(f"  Name: [bold]{v1.get('name', 'Unknown')}[/bold]")
        lines.append(f"  Address: {v1.get('address', 'N/A')}")

        # Get source info
        vendor1_obj = session.query(Vendor).filter(Vendor.id == v1.get("id")).first()
        if vendor1_obj:
            source1 = "CMBL" if vendor1_obj.in_cmbl else "LBB/Contract"
            lines.append(f"  Source: [magenta]{source1}[/magenta]")
            if vendor1_obj.vendor_id:
                lines.append(f"  State Vendor ID: {vendor1_obj.vendor_id}")

        lines.append("")
        lines.append("[bold green]Vendor 2:[/bold green]")
        lines.append(f"  Name: [bold]{v2.get('name', 'Unknown')}[/bold]")
        lines.append(f"  Address: {v2.get('address', 'N/A')}")

        vendor2_obj = session.query(Vendor).filter(Vendor.id == v2.get("id")).first()
        if vendor2_obj:
            source2 = "CMBL" if vendor2_obj.in_cmbl else "LBB/Contract"
            lines.append(f"  Source: [magenta]{source2}[/magenta]")
            if vendor2_obj.vendor_id:
                lines.append(f"  State Vendor ID: {vendor2_obj.vendor_id}")

        lines.append("")
        lines.append("[bold red]⚠ Red Flag:[/bold red] Very similar vendor names could indicate:")
        lines.append("  • Duplicate vendor entries")
        lines.append("  • Related party transactions")
        lines.append("  • Attempt to circumvent bidding requirements")

        return lines

    def _format_sequential_cluster(self, evidence: dict, session) -> list:
        """Format sequential vendor ID alert evidence."""
        lines = []
        lines.append("[bold cyan]═══ SEQUENTIAL VENDOR IDS ═══[/bold cyan]")
        lines.append("")

        similarity = evidence.get("similarity", 0) * 100
        lines.append(f"[bold]Name Similarity:[/bold] [yellow]{similarity:.1f}%[/yellow]")
        lines.append("")

        v1 = evidence.get("vendor1", {})
        v2 = evidence.get("vendor2", {})

        lines.append("[bold green]Vendor 1:[/bold green]")
        lines.append(f"  Name: [bold]{v1.get('name', 'Unknown')}[/bold]")
        lines.append(f"  Vendor ID: [yellow]{v1.get('vendor_id', 'N/A')}[/yellow]")

        lines.append("")
        lines.append("[bold green]Vendor 2:[/bold green]")
        lines.append(f"  Name: [bold]{v2.get('name', 'Unknown')}[/bold]")
        lines.append(f"  Vendor ID: [yellow]{v2.get('vendor_id', 'N/A')}[/yellow]")

        lines.append("")
        lines.append("[bold red]⚠ Red Flag:[/bold red] Sequential vendor IDs with similar names")
        lines.append("  suggests bulk registration, possibly to:")
        lines.append("  • Split contracts below bidding thresholds")
        lines.append("  • Create appearance of competition")
        lines.append("  • Avoid vendor consolidation rules")

        return lines

    def _format_contract_splitting(self, evidence: dict, session) -> list:
        """Format contract splitting alert evidence."""
        lines = []
        lines.append("[bold cyan]═══ POTENTIAL CONTRACT SPLITTING ═══[/bold cyan]")
        lines.append("")

        vendor_name = evidence.get("vendor_name", "Unknown")
        threshold = evidence.get("threshold_name", "Unknown threshold")
        contract_count = evidence.get("contract_count", 0)
        total_value = evidence.get("total_value", 0)
        avg_value = evidence.get("average_value", 0)

        lines.append(f"[bold]Vendor:[/bold] [yellow]{vendor_name}[/yellow]")
        lines.append(f"[bold]Threshold:[/bold] {threshold}")
        lines.append(f"[bold]Number of contracts:[/bold] {contract_count}")
        lines.append(f"[bold]Total value:[/bold] ${total_value:,.2f}")
        lines.append(f"[bold]Average value:[/bold] ${avg_value:,.2f}")
        lines.append("")

        lines.append("[bold green]Contracts (from LBB database):[/bold green]")
        lines.append("─" * 60)

        contracts = evidence.get("contracts", [])
        for c in contracts:
            number = c.get("number", "N/A")
            value = c.get("value", 0)
            start = c.get("start_date", "N/A")
            desc = c.get("description", "")[:50]

            lines.append(f"  [cyan]●[/cyan] Contract: [bold]{number}[/bold]")
            lines.append(f"    Value: ${value:,.2f}")
            lines.append(f"    Date: {start}")
            lines.append(f"    Description: {desc}")
            lines.append("")

        lines.append("[bold red]⚠ Red Flag:[/bold red] Multiple contracts just at/below threshold")
        lines.append("  suggests intentional splitting to avoid:")
        lines.append("  • LBB reporting requirements ($50K)")
        lines.append("  • Competitive bidding requirements")
        lines.append("  • Executive oversight")

        return lines


class VendorDetailModal(ModalScreen):
    """Modal to show detailed vendor information."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    VendorDetailModal {
        align: center middle;
    }

    #vendor-detail-container {
        width: 90%;
        height: 85%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #vendor-detail-title {
        text-style: bold;
        text-align: center;
        padding: 1;
        border-bottom: solid $primary;
    }

    #vendor-detail-content {
        height: auto;
        padding: 1;
    }

    #vendor-scroll {
        height: 1fr;
    }

    .close-hint {
        text-align: center;
        color: $text-muted;
        dock: bottom;
        padding: 1;
    }
    """

    def __init__(self, vendor_id: int, **kwargs):
        super().__init__(**kwargs)
        self.vendor_id = vendor_id

    def compose(self) -> ComposeResult:
        with Container(id="vendor-detail-container"):
            yield Static("", id="vendor-detail-title")
            yield VerticalScroll(Static("", id="vendor-detail-content"), id="vendor-scroll")
            yield Static("[dim]Press ESC or Q to close[/dim]", classes="close-hint")

    def on_mount(self) -> None:
        self.load_vendor_details()

    def load_vendor_details(self) -> None:
        """Load and display vendor details."""
        with get_session() as s:
            vendor = s.query(Vendor).filter(Vendor.id == self.vendor_id).first()
            if not vendor:
                self.query_one("#vendor-detail-title", Static).update("[red]Vendor not found[/red]")
                return

            title = f"[bold cyan]VENDOR DETAILS: {vendor.name}[/bold cyan]"
            self.query_one("#vendor-detail-title", Static).update(title)

            lines = []

            lines.append("[bold yellow]═══ BASIC INFORMATION ═══[/bold yellow]")
            lines.append("")
            lines.append(f"[bold]Vendor ID:[/bold] {vendor.vendor_id or 'N/A'}")
            lines.append(f"[bold]Name:[/bold] {vendor.name}")
            if vendor.address:
                lines.append(f"[bold]Address:[/bold] {vendor.address}")
            if vendor.city or vendor.state or vendor.zip_code:
                location = f"{vendor.city or ''}, {vendor.state or ''} {vendor.zip_code or ''}".strip(", ")
                lines.append(f"[bold]Location:[/bold] {location}")
            if vendor.phone:
                lines.append(f"[bold]Phone:[/bold] {vendor.phone}")
            lines.append("")

            lines.append("[bold yellow]═══ CERTIFICATION STATUS ═══[/bold yellow]")
            lines.append("")
            hub_display = normalize_hub_status(vendor.hub_status) if vendor.hub_status else "Not HUB Certified"
            lines.append(f"[bold]HUB Status:[/bold] [green]{hub_display}[/green]")

            if vendor.raw_data and "ELIGIBILITY CODE" in vendor.raw_data:
                lines.append(f"[bold]Eligibility Code:[/bold] {vendor.raw_data['ELIGIBILITY CODE']}")

            lines.append(f"[bold]CMBL Status:[/bold] {'[green]Yes (in CMBL)[/green]' if vendor.in_cmbl else '[dim]No[/dim]'}")

            if vendor.risk_score is not None:
                risk_color = "red" if vendor.risk_score >= 70 else "yellow" if vendor.risk_score >= 40 else "green"
                lines.append(f"[bold]Risk Score:[/bold] [{risk_color}]{vendor.risk_score}/100[/{risk_color}]")
            lines.append("")

            lines.append("[bold yellow]═══ ACTIVITY TIMELINE ═══[/bold yellow]")
            lines.append("")
            if vendor.first_seen:
                lines.append(f"[bold]First Seen:[/bold] {vendor.first_seen.strftime('%Y-%m-%d')}")
            if vendor.last_seen:
                lines.append(f"[bold]Last Seen:[/bold] {vendor.last_seen.strftime('%Y-%m-%d')}")
            lines.append("")

            contracts = s.query(Contract).filter(Contract.vendor_id == vendor.id).all()
            if contracts:
                total_contract_value = sum(float(c.current_value or 0) for c in contracts)
                lines.append(f"[bold yellow]═══ CONTRACTS ({len(contracts)}) ═══[/bold yellow]")
                lines.append(f"[bold]Total Contract Value:[/bold] [green]${total_contract_value:,.2f}[/green]")
                lines.append("")

                for c in contracts[:10]:  # Limit to 10 contracts
                    lines.append(f"  [cyan]●[/cyan] [bold]{c.contract_number or 'N/A'}[/bold]")
                    lines.append(f"    Value: ${float(c.current_value or 0):,.2f}")
                    if c.start_date and c.end_date:
                        lines.append(f"    Period: {c.start_date.strftime('%Y-%m-%d')} to {c.end_date.strftime('%Y-%m-%d')}")
                    if c.agency:
                        lines.append(f"    Agency: {c.agency.name}")
                    if c.description:
                        desc = (c.description[:60] + "...") if len(c.description) > 60 else c.description
                        lines.append(f"    Description: {desc}")
                    lines.append("")

                if len(contracts) > 10:
                    lines.append(f"[dim]  ... and {len(contracts) - 10} more contracts[/dim]")
                    lines.append("")
            else:
                lines.append("[bold yellow]═══ CONTRACTS ═══[/bold yellow]")
                lines.append("[dim]No contracts found[/dim]")
                lines.append("")

            payment_count = s.query(func.count(Payment.id)).filter(Payment.vendor_id == vendor.id).scalar() or 0
            total_payments = s.query(func.sum(Payment.amount)).filter(Payment.vendor_id == vendor.id).scalar() or 0

            lines.append("[bold yellow]═══ PAYMENT SUMMARY ═══[/bold yellow]")
            lines.append("")
            lines.append(f"[bold]Total Payments:[/bold] {payment_count:,}")
            lines.append(f"[bold]Total Amount:[/bold] [green]${float(total_payments):,.2f}[/green]")
            lines.append("")

            if vendor.nigp_codes:
                lines.append("[bold yellow]═══ NIGP CODES ═══[/bold yellow]")
                lines.append("")
                for code in vendor.nigp_codes[:10]:
                    lines.append(f"  [cyan]●[/cyan] {code}")
                if len(vendor.nigp_codes) > 10:
                    lines.append(f"[dim]  ... and {len(vendor.nigp_codes) - 10} more codes[/dim]")
                lines.append("")

            lines.append("[bold yellow]═══ SOURCE INFORMATION ═══[/bold yellow]")
            lines.append("")
            source = "CMBL" if vendor.in_cmbl else "LBB/Contracts/Other"
            lines.append(f"[bold]Data Source:[/bold] [magenta]{source}[/magenta]")
            lines.append(f"[bold]Created:[/bold] {vendor.created_at.strftime('%Y-%m-%d %H:%M') if vendor.created_at else 'Unknown'}")
            lines.append(f"[bold]Updated:[/bold] {vendor.updated_at.strftime('%Y-%m-%d %H:%M') if vendor.updated_at else 'Unknown'}")

            self.query_one("#vendor-detail-content", Static).update("\n".join(lines))


class PaymentDetailModal(ModalScreen):
    """Modal to show detailed payment information."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    PaymentDetailModal {
        align: center middle;
    }

    #payment-detail-container {
        width: 90%;
        height: 85%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #payment-detail-title {
        text-style: bold;
        text-align: center;
        padding: 1;
        border-bottom: solid $primary;
    }

    #payment-detail-content {
        height: auto;
        padding: 1;
    }

    #payment-scroll {
        height: 1fr;
    }

    .close-hint {
        text-align: center;
        color: $text-muted;
        dock: bottom;
        padding: 1;
    }
    """

    def __init__(self, payment_id: int, **kwargs):
        super().__init__(**kwargs)
        self.payment_id = payment_id

    def compose(self) -> ComposeResult:
        with Container(id="payment-detail-container"):
            yield Static("", id="payment-detail-title")
            yield VerticalScroll(Static("", id="payment-detail-content"), id="payment-scroll")
            yield Static("[dim]Press ESC or Q to close[/dim]", classes="close-hint")

    def on_mount(self) -> None:
        self.load_payment_details()

    def load_payment_details(self) -> None:
        """Load and display payment details."""
        with get_session() as s:
            payment = s.query(Payment).filter(Payment.id == self.payment_id).first()
            if not payment:
                self.query_one("#payment-detail-title", Static).update("[red]Payment not found[/red]")
                return

            title = f"[bold cyan]PAYMENT DETAILS: ${float(payment.amount):,.2f}[/bold cyan]"
            self.query_one("#payment-detail-title", Static).update(title)

            lines = []

            lines.append("[bold yellow]═══ PAYMENT INFORMATION ═══[/bold yellow]")
            lines.append("")
            lines.append(f"[bold]Payment ID:[/bold] {payment.id}")
            lines.append(f"[bold]Amount:[/bold] [green]${float(payment.amount):,.2f}[/green]")
            if payment.payment_date:
                lines.append(f"[bold]Payment Date:[/bold] {payment.payment_date.strftime('%Y-%m-%d')}")
            if payment.description:
                lines.append(f"[bold]Description:[/bold] {payment.description}")
            lines.append("")

            if payment.vendor:
                lines.append("[bold yellow]═══ VENDOR ═══[/bold yellow]")
                lines.append("")
                lines.append(f"[bold]Vendor Name:[/bold] {payment.vendor.name}")
                if payment.vendor.vendor_id:
                    lines.append(f"[bold]Vendor ID:[/bold] {payment.vendor.vendor_id}")
                if payment.vendor.address:
                    lines.append(f"[bold]Address:[/bold] {payment.vendor.address}")
                if payment.vendor.city or payment.vendor.state:
                    location = f"{payment.vendor.city or ''}, {payment.vendor.state or ''}".strip(", ")
                    lines.append(f"[bold]Location:[/bold] {location}")
                if payment.vendor.hub_status:
                    hub_display = normalize_hub_status(payment.vendor.hub_status)
                    lines.append(f"[bold]HUB Status:[/bold] [green]{hub_display}[/green]")
                lines.append("")

            if payment.agency:
                lines.append("[bold yellow]═══ AGENCY ═══[/bold yellow]")
                lines.append("")
                lines.append(f"[bold]Agency Name:[/bold] {payment.agency.name}")
                if payment.agency.agency_code:
                    lines.append(f"[bold]Agency Code:[/bold] {payment.agency.agency_code}")
                if payment.agency.category:
                    lines.append(f"[bold]Category:[/bold] {payment.agency.category}")
                lines.append("")

            lines.append("[bold yellow]═══ FISCAL INFORMATION ═══[/bold yellow]")
            lines.append("")
            if payment.fiscal_year_state:
                lines.append(f"[bold]State Fiscal Year:[/bold] FY{payment.fiscal_year_state}")
            if payment.fiscal_year_federal:
                lines.append(f"[bold]Federal Fiscal Year:[/bold] FY{payment.fiscal_year_federal}")
            if payment.calendar_year:
                lines.append(f"[bold]Calendar Year:[/bold] {payment.calendar_year}")
            if payment.comptroller_object_code:
                lines.append(f"[bold]Object Code:[/bold] {payment.comptroller_object_code}")
            lines.append("")

            lines.append("[bold yellow]═══ CLASSIFICATION ═══[/bold yellow]")
            lines.append("")
            confidential_status = "[red]YES[/red]" if payment.is_confidential else "[dim]No[/dim]"
            lines.append(f"[bold]Confidential:[/bold] {confidential_status}")
            if payment.source_system:
                lines.append(f"[bold]Source System:[/bold] [magenta]{payment.source_system}[/magenta]")
            if payment.source_id:
                lines.append(f"[bold]Source ID:[/bold] {payment.source_id}")
            lines.append("")

            if payment.raw_data:
                lines.append("[bold yellow]═══ RAW DATA FIELDS ═══[/bold yellow]")
                lines.append("")
                for key, value in list(payment.raw_data.items())[:15]:  # Limit to 15 fields
                    value_str = str(value)[:60]
                    if len(str(value)) > 60:
                        value_str += "..."
                    lines.append(f"  [cyan]{key}:[/cyan] {value_str}")
                if len(payment.raw_data) > 15:
                    lines.append(f"[dim]  ... and {len(payment.raw_data) - 15} more fields[/dim]")
                lines.append("")

            lines.append("[bold yellow]═══ RECORD METADATA ═══[/bold yellow]")
            lines.append("")
            lines.append(f"[bold]Created:[/bold] {payment.created_at.strftime('%Y-%m-%d %H:%M') if payment.created_at else 'Unknown'}")

            self.query_one("#payment-detail-content", Static).update("\n".join(lines))


class ContractDetailModal(ModalScreen):
    """Modal to show detailed contract information."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    CSS = """
    ContractDetailModal {
        align: center middle;
    }

    #contract-detail-container {
        width: 90%;
        height: 85%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #contract-detail-title {
        text-style: bold;
        text-align: center;
        padding: 1;
        border-bottom: solid $primary;
    }

    #contract-detail-content {
        height: auto;
        padding: 1;
    }

    #contract-scroll {
        height: 1fr;
    }

    .close-hint {
        text-align: center;
        color: $text-muted;
        dock: bottom;
        padding: 1;
    }
    """

    def __init__(self, contract_id: int, **kwargs):
        super().__init__(**kwargs)
        self.contract_id = contract_id

    def compose(self) -> ComposeResult:
        with Container(id="contract-detail-container"):
            yield Static("", id="contract-detail-title")
            yield VerticalScroll(Static("", id="contract-detail-content"), id="contract-scroll")
            yield Static("[dim]Press ESC or Q to close[/dim]", classes="close-hint")

    def on_mount(self) -> None:
        self.load_contract_details()

    def load_contract_details(self) -> None:
        """Load and display contract details."""
        with get_session() as s:
            contract = s.query(Contract).filter(Contract.id == self.contract_id).first()
            if not contract:
                self.query_one("#contract-detail-title", Static).update("[red]Contract not found[/red]")
                return

            title = f"[bold cyan]CONTRACT DETAILS: {contract.contract_number}[/bold cyan]"
            self.query_one("#contract-detail-title", Static).update(title)

            lines = []

            lines.append("[bold yellow]═══ CONTRACT INFORMATION ═══[/bold yellow]")
            lines.append("")
            lines.append(f"[bold]Contract Number:[/bold] {contract.contract_number}")
            if contract.current_value:
                lines.append(f"[bold]Current Value:[/bold] [green]${float(contract.current_value):,.2f}[/green]")
            if contract.max_value:
                lines.append(f"[bold]Maximum Value:[/bold] [green]${float(contract.max_value):,.2f}[/green]")
            if contract.description:
                lines.append(f"[bold]Description:[/bold]")
                desc = contract.description
                for i in range(0, len(desc), 80):
                    lines.append(f"  {desc[i:i+80]}")
            lines.append("")

            lines.append("[bold yellow]═══ CONTRACT PERIOD ═══[/bold yellow]")
            lines.append("")
            if contract.start_date:
                lines.append(f"[bold]Start Date:[/bold] {contract.start_date.strftime('%Y-%m-%d')}")
            if contract.end_date:
                lines.append(f"[bold]End Date:[/bold] {contract.end_date.strftime('%Y-%m-%d')}")
                from datetime import date
                if contract.end_date >= date.today():
                    days_left = (contract.end_date - date.today()).days
                    lines.append(f"[bold]Days Remaining:[/bold] [yellow]{days_left}[/yellow]")
                else:
                    lines.append(f"[bold]Status:[/bold] [red]Expired[/red]")
            if contract.fiscal_year:
                lines.append(f"[bold]Fiscal Year:[/bold] FY{contract.fiscal_year}")
            lines.append("")

            if contract.vendor:
                lines.append("[bold yellow]═══ VENDOR ═══[/bold yellow]")
                lines.append("")
                lines.append(f"[bold]Vendor Name:[/bold] {contract.vendor.name}")
                if contract.vendor.vendor_id:
                    lines.append(f"[bold]Vendor ID:[/bold] {contract.vendor.vendor_id}")
                if contract.vendor.address:
                    lines.append(f"[bold]Address:[/bold] {contract.vendor.address}")
                if contract.vendor.city or contract.vendor.state:
                    location = f"{contract.vendor.city or ''}, {contract.vendor.state or ''}".strip(", ")
                    lines.append(f"[bold]Location:[/bold] {location}")
                if contract.vendor.hub_status:
                    hub_display = normalize_hub_status(contract.vendor.hub_status)
                    lines.append(f"[bold]HUB Status:[/bold] [green]{hub_display}[/green]")
                if contract.vendor.in_cmbl:
                    lines.append(f"[bold]CMBL Status:[/bold] [green]Yes (in CMBL)[/green]")
                lines.append("")

            if contract.agency:
                lines.append("[bold yellow]═══ AGENCY ═══[/bold yellow]")
                lines.append("")
                lines.append(f"[bold]Agency Name:[/bold] {contract.agency.name}")
                if contract.agency.agency_code:
                    lines.append(f"[bold]Agency Code:[/bold] {contract.agency.agency_code}")
                if contract.agency.category:
                    lines.append(f"[bold]Category:[/bold] {contract.agency.category}")
                lines.append("")

            if contract.nigp_codes:
                lines.append("[bold yellow]═══ NIGP CODES ═══[/bold yellow]")
                lines.append("")
                for code in contract.nigp_codes[:10]:
                    lines.append(f"  [cyan]●[/cyan] {code}")
                if len(contract.nigp_codes) > 10:
                    lines.append(f"[dim]  ... and {len(contract.nigp_codes) - 10} more codes[/dim]")
                lines.append("")

            lines.append("[bold yellow]═══ SOURCE INFORMATION ═══[/bold yellow]")
            lines.append("")
            if contract.source:
                lines.append(f"[bold]Data Source:[/bold] [magenta]{contract.source}[/magenta]")
            lines.append(f"[bold]Created:[/bold] {contract.created_at.strftime('%Y-%m-%d %H:%M') if contract.created_at else 'Unknown'}")
            lines.append(f"[bold]Updated:[/bold] {contract.updated_at.strftime('%Y-%m-%d %H:%M') if contract.updated_at else 'Unknown'}")
            lines.append("")

            if contract.raw_data:
                lines.append("[bold yellow]═══ RAW DATA FIELDS ═══[/bold yellow]")
                lines.append("")
                for key, value in list(contract.raw_data.items())[:15]:  # Limit to 15 fields
                    value_str = str(value)[:60]
                    if len(str(value)) > 60:
                        value_str += "..."
                    lines.append(f"  [cyan]{key}:[/cyan] {value_str}")
                if len(contract.raw_data) > 15:
                    lines.append(f"[dim]  ... and {len(contract.raw_data) - 15} more fields[/dim]")

            self.query_one("#contract-detail-content", Static).update("\n".join(lines))


class AlertsScreen(Container):
    """Alerts management view with pagination."""

    BINDINGS = [
        Binding("pageup", "prev_page", "Previous Page"),
        Binding("pagedown", "next_page", "Next Page"),
        Binding("[", "prev_page", "Previous Page"),
        Binding("]", "next_page", "Next Page"),
    ]

    PAGE_SIZE = 50

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_page = 0
        self.total_alerts = 0
        self.current_severity = None

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Button("All", id="alerts-all-btn", variant="default"),
            Button("High", id="alerts-high-btn", variant="error"),
            Button("Medium", id="alerts-med-btn", variant="warning"),
            Button("◀ Prev", id="alerts-prev-btn", variant="primary"),
            Button("Next ▶", id="alerts-next-btn", variant="primary"),
            Button("Refresh", id="alerts-refresh-btn", variant="success"),
            classes="controls",
        )
        yield Static("", id="alerts-pagination-info")
        yield DataTable(id="alerts-table")

    def on_mount(self) -> None:
        table = self.query_one("#alerts-table", DataTable)
        table.add_columns("ID", "Sev", "Type", "Title", "Status", "Created")
        table.cursor_type = "row"
        table.zebra_stripes = True
        self.load_alerts()

    def load_alerts(self, severity: str = None, reset_page: bool = True) -> None:
        if reset_page:
            self.current_page = 0
        self.current_severity = severity

        table = self.query_one("#alerts-table", DataTable)
        table.clear()

        with get_session() as s:
            # Build base query
            query = s.query(Alert)

            # Apply filters BEFORE limit
            if severity == "high":
                query = query.filter(Alert.severity == AlertSeverity.HIGH)
            elif severity == "medium":
                query = query.filter(Alert.severity == AlertSeverity.MEDIUM)

            # Get total count for pagination
            self.total_alerts = query.count()
            total_pages = max(1, (self.total_alerts + self.PAGE_SIZE - 1) // self.PAGE_SIZE)

            # Update pagination info
            start_idx = self.current_page * self.PAGE_SIZE + 1
            end_idx = min((self.current_page + 1) * self.PAGE_SIZE, self.total_alerts)
            page_info = f"[cyan]Page {self.current_page + 1}/{total_pages}[/cyan] | Showing {start_idx}-{end_idx} of {self.total_alerts:,} alerts | [dim]PageUp/PageDown or [ ] to navigate[/dim]"
            self.query_one("#alerts-pagination-info", Static).update(page_info)

            # Apply ordering, offset and limit
            offset = self.current_page * self.PAGE_SIZE
            query = query.order_by(
                Alert.severity.desc(),
                Alert.created_at.desc()
            ).offset(offset).limit(self.PAGE_SIZE)

            for alert in query.all():
                if alert.severity == AlertSeverity.HIGH:
                    sev = "[red]HIGH[/red]"
                elif alert.severity == AlertSeverity.MEDIUM:
                    sev = "[yellow]MED[/yellow]"
                else:
                    sev = "[blue]LOW[/blue]"

                created = alert.created_at.strftime("%Y-%m-%d") if alert.created_at else "-"

                table.add_row(
                    str(alert.id),
                    sev,
                    alert.alert_type or "-",
                    (alert.title or "Untitled")[:40],
                    alert.status.value if alert.status else "-",
                    created,
                    key=str(alert.id),
                )

    def action_prev_page(self) -> None:
        """Go to previous page."""
        if self.current_page > 0:
            self.current_page -= 1
            self.load_alerts(severity=self.current_severity, reset_page=False)

    def action_next_page(self) -> None:
        """Go to next page."""
        total_pages = max(1, (self.total_alerts + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_alerts(severity=self.current_severity, reset_page=False)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle double-click on alert row to show details."""
        if event.row_key:
            alert_id = int(event.row_key.value)
            self.app.push_screen(AlertDetailModal(alert_id))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "alerts-all-btn":
            self.load_alerts()
        elif event.button.id == "alerts-high-btn":
            self.load_alerts(severity="high")
        elif event.button.id == "alerts-med-btn":
            self.load_alerts(severity="medium")
        elif event.button.id == "alerts-prev-btn":
            self.action_prev_page()
        elif event.button.id == "alerts-next-btn":
            self.action_next_page()
        elif event.button.id == "alerts-refresh-btn":
            self.load_alerts(severity=self.current_severity)


class StatsScreen(ScrollableContainer):
    """Statistics and visualizations screen."""

    def compose(self) -> ComposeResult:
        # === Overview ===
        yield Static("[b]OVERVIEW[/b]", classes="section-title")
        yield Static("", id="summary-stats", classes="summary-box")
        yield Rule()

        # === Spending Analysis ===
        yield Static("[b]TOP AGENCIES BY SPENDING[/b]", classes="section-title")
        yield Static("", id="agency-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]PAYMENTS BY FISCAL YEAR[/b]", classes="section-title")
        yield Static("", id="fy-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]PAYMENT SIZE DISTRIBUTION[/b]", classes="section-title")
        yield Static("", id="payment-size-chart", classes="chart-box")
        yield Rule()

        # === HUB Program ===
        yield Static("[b cyan]═══ HUB PROGRAM ANALYSIS ═══[/b cyan]", classes="section-title")
        yield Rule()
        yield Static("[b]HUB VENDOR DISTRIBUTION[/b]", classes="section-title")
        yield Static("", id="hub-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]HUB vs NON-HUB SPENDING[/b]", classes="section-title")
        yield Static("", id="hub-vs-nonhub-chart", classes="chart-box")
        yield Rule()

        # === Vendors ===
        yield Static("[b]VENDOR STATE DISTRIBUTION[/b]", classes="section-title")
        yield Static("", id="vendor-state-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]CONTRACT DURATION[/b]", classes="section-title")
        yield Static("", id="contract-duration-chart", classes="chart-box")
        yield Rule()

        # === Alerts ===
        yield Static("[b yellow]═══ ALERTS & RISK ═══[/b yellow]", classes="section-title")
        yield Rule()
        yield Static("[b]ALERT DISTRIBUTION BY TYPE[/b]", classes="section-title")
        yield Static("", id="alert-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]ALERTS BY SEVERITY[/b]", classes="section-title")
        yield Static("", id="agency-risk-chart", classes="chart-box")
        yield Rule()

        # === Debarment (SAM.gov) ===
        yield Static("[b red]═══ DEBARMENT SCREENING ═══[/b red]", classes="section-title")
        yield Rule()
        yield Static("[b]SAM.GOV EXCLUSIONS[/b]", classes="section-title")
        yield Static("", id="debarment-summary", classes="chart-box")
        yield Rule()
        yield Static("[b]DEBARMENT ALERTS[/b]", classes="section-title")
        yield Static("", id="debarment-alerts", classes="chart-box")
        yield Rule()

        # === Cross-Reference Detection ===
        yield Static("[b magenta]═══ CROSS-REFERENCE DETECTION ═══[/b magenta]", classes="section-title")
        yield Rule()
        yield Static("[b]EMPLOYEE-VENDOR CONFLICTS[/b]", classes="section-title")
        yield Static("", id="employee-vendor-matches", classes="chart-box")
        yield Rule()
        yield Static("[b]PAY-TO-PLAY DETECTION[/b]", classes="section-title")
        yield Static("", id="pay-to-play-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]GHOST VENDOR INDICATORS[/b]", classes="section-title")
        yield Static("", id="ghost-vendor-chart", classes="chart-box")
        yield Rule()
        yield Static("[b]FISCAL YEAR-END SPENDING[/b]", classes="section-title")
        yield Static("", id="fy-end-spending-chart", classes="chart-box")

    def on_mount(self) -> None:
        self.refresh_stats()

    def refresh_stats(self) -> None:
        """Refresh all stats and charts."""
        with get_session() as s:
            # === Summary Stats ===
            total_vendors = s.query(func.count(Vendor.id)).scalar() or 0
            hub_vendors = s.query(func.count(Vendor.id)).filter(
                Vendor.hub_status.isnot(None),
                Vendor.hub_status != "",
                Vendor.hub_status != "Non HUB",
                Vendor.hub_status != "N"
            ).scalar() or 0
            total_payments = s.query(func.count(Payment.id)).scalar() or 0
            total_spending = float(s.query(func.sum(Payment.amount)).scalar() or 0)
            total_contracts = s.query(func.count(Contract.id)).scalar() or 0
            contract_value = float(s.query(func.sum(Contract.current_value)).scalar() or 0)
            total_alerts = s.query(func.count(Alert.id)).scalar() or 0

            # Count debarred entities if available
            debarred_count = 0
            if HAS_EXTENDED_MODELS and DebarredEntity is not None:
                try:
                    debarred_count = s.query(func.count(DebarredEntity.id)).filter(
                        DebarredEntity.is_active == True
                    ).scalar() or 0
                except Exception:
                    pass

            summary_text = (
                f"[cyan]Vendors:[/cyan] {total_vendors:,}  "
                f"[green]HUB:[/green] {hub_vendors:,}  "
                f"[yellow]Payments:[/yellow] {total_payments:,}  "
                f"[magenta]Spending:[/magenta] ${total_spending/1e9:.2f}B\n"
                f"[cyan]Contracts:[/cyan] {total_contracts:,}  "
                f"[green]Value:[/green] ${contract_value/1e9:.2f}B  "
                f"[red]Alerts:[/red] {total_alerts:,}  "
                f"[yellow]Exclusions:[/yellow] {debarred_count:,}"
            )
            self.query_one("#summary-stats", Static).update(summary_text)

            # === HUB Status Distribution ===
            ELIGIBILITY_MAP = {
                "HI": "Hispanic", "BL": "Black American", "WO": "Woman Owned",
                "AS": "Asian Pacific", "AI": "American Indian", "DV": "Disabled Veteran",
            }

            hub_vendors_all = s.query(Vendor).filter(
                Vendor.hub_status.isnot(None),
                Vendor.hub_status != "",
                Vendor.hub_status != "Non HUB",
                Vendor.hub_status != "N"
            ).all()

            hub_counts = defaultdict(int)
            for v in hub_vendors_all:
                status = v.hub_status
                if status == "X" and v.raw_data and "ELIGIBILITY CODE" in v.raw_data:
                    elig_code = v.raw_data["ELIGIBILITY CODE"]
                    if elig_code in ELIGIBILITY_MAP:
                        hub_counts[ELIGIBILITY_MAP[elig_code]] += 1
                    else:
                        hub_counts["Other"] += 1
                else:
                    normalized = normalize_hub_status(status)
                    if normalized not in ("Non-HUB", "Unknown", "Multiple Certs"):
                        hub_counts[normalized] += 1

            hub_data = sorted(hub_counts.items(), key=lambda x: -x[1])[:10]

            if hub_data:
                hub_chart = create_ascii_bar_chart(hub_data, title="HUB Vendors by Category", horizontal=True)
                self.query_one("#hub-chart", Static).update(hub_chart)
            else:
                self.query_one("#hub-chart", Static).update("[dim]No HUB data available[/dim]")

            # === Top Agencies by Spending ===
            top_agencies = s.query(
                Agency.name,
                func.sum(Payment.amount).label("total")
            ).join(Payment).group_by(Agency.id).order_by(
                func.sum(Payment.amount).desc()
            ).limit(10).all()

            if top_agencies:
                agency_data = [((a[0] or "Unknown")[:25], float(a[1] or 0) / 1e9) for a in top_agencies]
                agency_chart = create_ascii_bar_chart(agency_data, title="Top Agencies ($B)", horizontal=True, value_suffix="B")
                self.query_one("#agency-chart", Static).update(agency_chart)
            else:
                self.query_one("#agency-chart", Static).update("[dim]No payment data[/dim]")

            # === Payments by Fiscal Year ===
            fy_data = s.query(
                Payment.fiscal_year_state,
                func.sum(Payment.amount)
            ).filter(
                Payment.fiscal_year_state.isnot(None)
            ).group_by(Payment.fiscal_year_state).order_by(Payment.fiscal_year_state).all()

            if fy_data:
                fy_chart_data = [(f"FY{d[0]}", float(d[1] or 0) / 1e9) for d in fy_data[-8:]]  # Last 8 years
                fy_chart = create_ascii_bar_chart(fy_chart_data, title="Spending by FY ($B)", horizontal=True, value_suffix="B")
                self.query_one("#fy-chart", Static).update(fy_chart)
            else:
                self.query_one("#fy-chart", Static).update("[dim]No fiscal year data[/dim]")

            # === Payment Size Distribution ===
            size_buckets = [
                ("$0-1K", 0, 1000), ("$1K-10K", 1000, 10000),
                ("$10K-100K", 10000, 100000), ("$100K-1M", 100000, 1000000),
                ("$1M+", 1000000, float('inf'))
            ]
            payment_size_data = []
            for label, min_val, max_val in size_buckets:
                if max_val == float('inf'):
                    count = s.query(func.count(Payment.id)).filter(Payment.amount >= min_val).scalar() or 0
                else:
                    count = s.query(func.count(Payment.id)).filter(Payment.amount >= min_val, Payment.amount < max_val).scalar() or 0
                payment_size_data.append((label, count))

            if any(count > 0 for _, count in payment_size_data):
                payment_size_chart = create_ascii_bar_chart(
                    payment_size_data,
                    title="Payment Distribution by Size",
                    horizontal=True
                )
                self.query_one("#payment-size-chart", Static).update(payment_size_chart)

            # === Contract Duration Analysis ===
            contracts_with_dates = s.query(Contract).filter(
                Contract.start_date.isnot(None),
                Contract.end_date.isnot(None)
            ).all()

            duration_buckets = {
                "< 1 year": 0,
                "1-2 years": 0,
                "2-5 years": 0,
                "5+ years": 0
            }

            for contract in contracts_with_dates:
                days = (contract.end_date - contract.start_date).days
                years = days / 365.25

                if years < 1:
                    duration_buckets["< 1 year"] += 1
                elif years < 2:
                    duration_buckets["1-2 years"] += 1
                elif years < 5:
                    duration_buckets["2-5 years"] += 1
                else:
                    duration_buckets["5+ years"] += 1

            duration_data = list(duration_buckets.items())
            if any(count > 0 for _, count in duration_data):
                duration_chart = create_ascii_bar_chart(
                    duration_data,
                    title="Contract Duration Distribution",
                    horizontal=True
                )
                self.query_one("#contract-duration-chart", Static).update(duration_chart)

            # === Vendor State Distribution ===
            state_data = s.query(
                Vendor.state,
                func.count(Vendor.id)
            ).filter(
                Vendor.state.isnot(None),
                Vendor.state != ""
            ).group_by(Vendor.state).order_by(
                func.count(Vendor.id).desc()
            ).limit(10).all()

            if state_data:
                state_chart_data = [(state or "Unknown", count) for state, count in state_data]
                state_chart = create_ascii_bar_chart(
                    state_chart_data,
                    title="Top 10 States by Vendor Count",
                    horizontal=True
                )
                self.query_one("#vendor-state-chart", Static).update(state_chart)

            # === Alert Distribution ===
            alert_types = s.query(
                Alert.alert_type,
                func.count(Alert.id)
            ).group_by(Alert.alert_type).all()

            if alert_types:
                alert_data = [(a[0].replace("_", " ").title()[:20] if a[0] else "Unknown", a[1]) for a in alert_types]
                alert_chart = create_ascii_bar_chart(
                    alert_data,
                    title="Alerts by Type",
                    horizontal=True
                )
                self.query_one("#alert-chart", Static).update(alert_chart)

            # === Alert Severity Breakdown ===
            severity_data = s.query(
                Alert.severity,
                func.count(Alert.id).label("count")
            ).group_by(Alert.severity).all()

            if severity_data:
                severity_labels = {
                    AlertSeverity.HIGH: "[red]HIGH[/red]",
                    AlertSeverity.MEDIUM: "[yellow]MEDIUM[/yellow]",
                    AlertSeverity.LOW: "[dim]LOW[/dim]",
                }
                severity_chart_data = [
                    (severity_labels.get(sev, str(sev)), count)
                    for sev, count in sorted(severity_data, key=lambda x: x[1], reverse=True)
                ]
                severity_chart = create_ascii_bar_chart(
                    severity_chart_data,
                    title="Alerts by Severity",
                    horizontal=True
                )
                self.query_one("#agency-risk-chart", Static).update(severity_chart)
            else:
                self.query_one("#agency-risk-chart", Static).update("[dim]No alerts - run detection analysis[/dim]")

            # === HUB vs Non-HUB Vendor Count ===
            # Note: Payment-vendor linkage not available, showing vendor counts instead
            hub_vendors = s.query(func.count(Vendor.id)).filter(
                Vendor.hub_status.isnot(None),
                Vendor.hub_status != "",
                ~Vendor.hub_status.in_(["N", "N/A", "Non HUB", "X"])
            ).scalar() or 0

            nonhub_vendors = s.query(func.count(Vendor.id)).filter(
                (Vendor.hub_status.is_(None)) |
                (Vendor.hub_status == "") |
                (Vendor.hub_status.in_(["N", "N/A", "Non HUB", "X"]))
            ).scalar() or 0

            if hub_vendors or nonhub_vendors:
                total = hub_vendors + nonhub_vendors
                hub_pct = (hub_vendors / total * 100) if total > 0 else 0
                hub_vs_data = [
                    (f"HUB ({hub_pct:.1f}%)", hub_vendors),
                    (f"Non-HUB ({100-hub_pct:.1f}%)", nonhub_vendors)
                ]
                hub_vs_chart = create_ascii_bar_chart(
                    hub_vs_data,
                    title="HUB vs Non-HUB Vendors",
                    horizontal=True
                )
                self.query_one("#hub-vs-nonhub-chart", Static).update(hub_vs_chart)
            else:
                self.query_one("#hub-vs-nonhub-chart", Static).update("[dim]No vendor data[/dim]")

            # ══════════════════════════════════════════════════════════════
            # CROSS-REFERENCE DETECTION
            # ══════════════════════════════════════════════════════════════

            # === Employee-Vendor Name Matches ===
            if HAS_EXTENDED_MODELS and EntityMatch is not None:
                try:
                    emp_vendor_matches = s.query(func.count(EntityMatch.id)).filter(
                        EntityMatch.entity_type_1 == "employee",
                        EntityMatch.entity_type_2 == "vendor"
                    ).scalar() or 0

                    high_conf_matches = s.query(func.count(EntityMatch.id)).filter(
                        EntityMatch.entity_type_1 == "employee",
                        EntityMatch.entity_type_2 == "vendor",
                        EntityMatch.confidence_score >= 0.9
                    ).scalar() or 0

                    if emp_vendor_matches > 0:
                        match_text = (
                            f"[red]⚠ POTENTIAL CONFLICTS OF INTEREST DETECTED[/red]\n\n"
                            f"[cyan]Total Employee-Vendor Matches:[/cyan] {emp_vendor_matches:,}\n"
                            f"[red]High Confidence (≥90%):[/red] {high_conf_matches:,}\n\n"
                            f"[dim]These are employees whose names closely match vendor names.\n"
                            f"This may indicate self-dealing or conflict of interest.[/dim]"
                        )
                    else:
                        match_text = "[green]No employee-vendor name matches detected[/green]\n[dim]Run detection analysis to find matches[/dim]"
                    self.query_one("#employee-vendor-matches", Static).update(match_text)
                except Exception:
                    self.query_one("#employee-vendor-matches", Static).update("[dim]Data not available[/dim]")
            else:
                self.query_one("#employee-vendor-matches", Static).update("[dim]Waiting for data...[/dim]")

            # === Pay-to-Play Detection ===
            if HAS_EXTENDED_MODELS and CampaignContribution is not None:
                try:
                    # Find vendors who are also campaign contributors
                    vendor_contributor_matches = s.query(
                        Vendor.name,
                        func.sum(CampaignContribution.contribution_amount).label("contrib_total"),
                        func.sum(Payment.amount).label("payment_total")
                    ).join(
                        CampaignContribution,
                        Vendor.name_normalized == CampaignContribution.contributor_normalized
                    ).outerjoin(
                        Payment, Payment.vendor_id == Vendor.id
                    ).group_by(Vendor.id, Vendor.name).having(
                        func.sum(Payment.amount) > 10000
                    ).order_by(desc("payment_total")).limit(10).all()

                    if vendor_contributor_matches:
                        # Create bar chart showing payments received
                        p2p_payment_data = [
                            (name[:20] if name else "Unknown", float(payments or 0) / 1e6)
                            for name, _, payments in vendor_contributor_matches
                        ]
                        p2p_chart = create_ascii_bar_chart(
                            p2p_payment_data,
                            title="Pay-to-Play: Payments to Contributors ($M)",
                            horizontal=True,
                            value_suffix="M"
                        )

                        # Add summary with contribution totals
                        total_contrib = sum(float(c or 0) for _, c, _ in vendor_contributor_matches)
                        total_payments = sum(float(p or 0) for _, _, p in vendor_contributor_matches)

                        p2p_text = f"[red]⚠ {len(vendor_contributor_matches)} VENDORS ARE CAMPAIGN CONTRIBUTORS[/red]\n\n"
                        p2p_text += f"[yellow]Total Contributions Made:[/yellow] ${total_contrib:,.0f}\n"
                        p2p_text += f"[cyan]Total Payments Received:[/cyan] ${total_payments:,.0f}\n"
                        p2p_text += f"[magenta]ROI Ratio:[/magenta] {total_payments/total_contrib:.1f}x\n\n" if total_contrib > 0 else "\n"
                        p2p_text += p2p_chart
                    else:
                        p2p_text = "[green]No obvious pay-to-play patterns detected[/green]\n[dim]Vendors are not matching campaign contributors[/dim]"
                    self.query_one("#pay-to-play-chart", Static).update(p2p_text)
                except Exception as e:
                    self.query_one("#pay-to-play-chart", Static).update(f"[dim]Analysis pending data sync[/dim]")
            else:
                self.query_one("#pay-to-play-chart", Static).update("[dim]Waiting for data...[/dim]")

            # === Ghost Vendor Indicators ===
            try:
                # Vendors not in CMBL
                non_cmbl = s.query(func.count(Vendor.id)).filter(
                    Vendor.in_cmbl != True
                ).scalar() or 0

                # Vendors with no address
                no_address = s.query(func.count(Vendor.id)).filter(
                    (Vendor.address.is_(None)) | (Vendor.address == "")
                ).scalar() or 0

                # Vendors receiving payments but not in CMBL
                paid_non_cmbl = s.query(func.count(func.distinct(Payment.vendor_id))).join(
                    Vendor, Payment.vendor_id == Vendor.id
                ).filter(Vendor.in_cmbl != True).scalar() or 0

                # In CMBL vendors
                in_cmbl = s.query(func.count(Vendor.id)).filter(
                    Vendor.in_cmbl == True
                ).scalar() or 0

                # Create bar chart for ghost vendor indicators
                ghost_data = [
                    ("In CMBL", in_cmbl),
                    ("NOT in CMBL", non_cmbl),
                    ("No Address", no_address),
                    ("Paid, NOT CMBL", paid_non_cmbl),
                ]
                ghost_chart = create_ascii_bar_chart(
                    ghost_data,
                    title="Ghost Vendor Risk Indicators",
                    horizontal=True
                )

                ghost_text = f"[red]GHOST VENDOR RISK ANALYSIS[/red]\n\n"
                ghost_text += ghost_chart
                ghost_text += f"\n[dim]Ghost vendors may be fictitious entities used for fraud[/dim]"
                self.query_one("#ghost-vendor-chart", Static).update(ghost_text)
            except Exception:
                self.query_one("#ghost-vendor-chart", Static).update("[dim]Data not available[/dim]")

            # === Fiscal Year End Spending Analysis ===
            try:
                # Texas FY ends August 31, so look at Aug-Sep spending
                aug_spending = s.query(func.sum(Payment.amount)).filter(
                    func.to_char(Payment.payment_date, 'MM') == '08'
                ).scalar() or 0

                sep_spending = s.query(func.sum(Payment.amount)).filter(
                    func.to_char(Payment.payment_date, 'MM') == '09'
                ).scalar() or 0

                # Compare to average monthly spending
                avg_monthly = s.query(func.sum(Payment.amount)).scalar() or 0
                avg_monthly = float(avg_monthly) / 12 if avg_monthly else 0

                aug_ratio = float(aug_spending) / avg_monthly if avg_monthly > 0 else 0
                sep_ratio = float(sep_spending) / avg_monthly if avg_monthly > 0 else 0

                fy_end_text = (
                    f"[cyan]Texas Fiscal Year ends August 31[/cyan]\n\n"
                    f"[yellow]August Spending:[/yellow] ${float(aug_spending)/1e9:.2f}B "
                    f"({'[red]' if aug_ratio > 1.5 else '[green]'}{aug_ratio:.1f}x avg[/])\n"
                    f"[yellow]September Spending:[/yellow] ${float(sep_spending)/1e9:.2f}B "
                    f"({'[red]' if sep_ratio > 1.5 else '[green]'}{sep_ratio:.1f}x avg[/])\n"
                    f"[dim]Average Monthly:[/dim] ${avg_monthly/1e9:.2f}B\n\n"
                )
                if aug_ratio > 1.5 or sep_ratio > 1.5:
                    fy_end_text += "[red]⚠ ELEVATED END-OF-YEAR SPENDING DETECTED[/red]\n"
                    fy_end_text += "[dim]May indicate 'use it or lose it' budget behavior[/dim]"
                else:
                    fy_end_text += "[green]Spending patterns appear normal[/green]"
                self.query_one("#fy-end-spending-chart", Static).update(fy_end_text)
            except Exception:
                self.query_one("#fy-end-spending-chart", Static).update("[dim]Data not available[/dim]")

            # ══════════════════════════════════════════════════════════════
            # DEBARMENT SCREENING
            # ══════════════════════════════════════════════════════════════

            # === SAM.gov Exclusions Summary ===
            if HAS_EXTENDED_MODELS and DebarredEntity is not None:
                try:
                    total_exclusions = s.query(func.count(DebarredEntity.id)).scalar() or 0
                    active_exclusions = s.query(func.count(DebarredEntity.id)).filter(
                        DebarredEntity.is_active == True
                    ).scalar() or 0
                    sam_gov_count = s.query(func.count(DebarredEntity.id)).filter(
                        DebarredEntity.source == "sam_gov"
                    ).scalar() or 0

                    if total_exclusions > 0:
                        debarment_text = (
                            f"[cyan]Total Exclusion Records:[/cyan] {total_exclusions:,}\n"
                            f"[red]Currently Active:[/red] {active_exclusions:,}\n"
                            f"[yellow]SAM.gov Federal Exclusions:[/yellow] {sam_gov_count:,}\n\n"
                            f"[dim]These are federally debarred, suspended, or excluded\n"
                            f"entities that should not receive government contracts.[/dim]"
                        )
                    else:
                        debarment_text = "[dim]No exclusion data yet. Run sync with sam_exclusions source.[/dim]"
                    self.query_one("#debarment-summary", Static).update(debarment_text)
                except Exception:
                    self.query_one("#debarment-summary", Static).update("[dim]Data not available[/dim]")
            else:
                self.query_one("#debarment-summary", Static).update("[dim]Waiting for data...[/dim]")

            # === Debarment Alerts ===
            try:
                debarment_alerts = s.query(func.count(Alert.id)).filter(
                    Alert.alert_type == "debarred_vendor"
                ).scalar() or 0

                high_sev = s.query(func.count(Alert.id)).filter(
                    Alert.alert_type == "debarred_vendor",
                    Alert.severity == AlertSeverity.HIGH
                ).scalar() or 0

                if debarment_alerts > 0:
                    # Get some example matches
                    sample_alerts = s.query(Alert).filter(
                        Alert.alert_type == "debarred_vendor"
                    ).order_by(Alert.created_at.desc()).limit(5).all()

                    alert_text = (
                        f"[red]⚠ DEBARRED VENDOR ALERTS: {debarment_alerts:,}[/red]\n"
                        f"[red]High Severity:[/red] {high_sev:,}\n\n"
                        f"[yellow]Recent Matches:[/yellow]\n"
                    )
                    for alert in sample_alerts:
                        alert_text += f"  • {alert.title[:60]}...\n" if len(alert.title) > 60 else f"  • {alert.title}\n"
                else:
                    alert_text = (
                        "[green]No debarment alerts[/green]\n"
                        "[dim]Run detection after syncing SAM.gov data to check vendors.[/dim]"
                    )
                self.query_one("#debarment-alerts", Static).update(alert_text)
            except Exception:
                self.query_one("#debarment-alerts", Static).update("[dim]Data not available[/dim]")


class FrauditApp(App):
    """Fraudit Terminal UI."""

    CSS = """
    /* Layout only - colors from system theme */

    .panel {
        border: round $primary;
        padding: 1 2;
        margin: 1;
    }

    .panel-title {
        text-style: bold;
        padding-bottom: 1;
    }

    #dashboard-grid {
        height: 100%;
    }

    #left-col {
        width: 45%;
    }

    #right-col {
        width: 55%;
    }

    .stats-row {
        height: auto;
        padding: 1 0;
    }

    StatCard {
        width: 1fr;
        height: auto;
        border: round $surface;
        padding: 0 1;
        margin: 0 1;
    }

    .stat-icon, .stat-title, .stat-value {
        text-align: center;
    }

    .stat-title, .stat-value {
        text-style: bold;
    }

    .total-spending {
        padding: 1;
        text-align: center;
        margin-top: 1;
    }

    .sync-list, .alert-list, .vendor-list {
        padding: 1 0;
    }

    .alert-badges {
        height: 3;
        padding: 0 0 1 0;
    }

    .alert-badge {
        width: 1fr;
        text-align: center;
        padding: 0 1;
    }

    .controls {
        height: 3;
        dock: top;
        padding: 0 1;
    }

    .controls Input {
        width: 20;
        margin-right: 1;
    }

    .controls Button {
        margin-right: 1;
    }

    DataTable {
        height: 100%;
    }

    TabbedContent {
        height: 100%;
    }

    /* Stats screen styles */
    .section-title {
        text-style: bold;
        padding: 1 2;
    }

    .summary-box {
        padding: 1 2;
    }

    .chart-box {
        padding: 0 2;
        min-height: 18;
        max-height: 22;
    }

    StatsScreen {
        padding: 1;
    }
    """

    BINDINGS = [
        Binding("d", "switch_tab('dashboard')", "Dashboard", show=True),
        Binding("t", "switch_tab('stats')", "Stats", show=True),
        Binding("v", "switch_tab('vendors')", "Vendors", show=True),
        Binding("p", "switch_tab('payments')", "Payments", show=True),
        Binding("c", "switch_tab('contracts')", "Contracts", show=True),
        Binding("a", "switch_tab('alerts')", "Alerts", show=True),
        Binding("r", "refresh", "Refresh", show=True),
        Binding("s", "sync", "Sync", show=True),
        Binding("q", "quit", "Quit", show=True),
    ]

    TITLE = "Fraudit"
    SUB_TITLE = "Government Spending Fraud Detection"

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(initial="dashboard"):
            with TabPane("Dashboard", id="dashboard"):
                yield DashboardScreen()
            with TabPane("Stats", id="stats"):
                yield StatsScreen()
            with TabPane("Vendors", id="vendors"):
                yield VendorsScreen()
            with TabPane("Payments", id="payments"):
                yield PaymentsScreen()
            with TabPane("Contracts", id="contracts"):
                yield ContractsScreen()
            with TabPane("Alerts", id="alerts"):
                yield AlertsScreen()
        yield Footer()

    def action_switch_tab(self, tab_id: str) -> None:
        """Switch to the specified tab."""
        tabs = self.query_one(TabbedContent)
        tabs.active = tab_id

    def action_refresh(self) -> None:
        """Refresh current view."""
        try:
            for widget_class in [StatsPanel, SyncPanel, AlertsPanel, TopVendorsPanel, StatsScreen]:
                for widget in self.query(widget_class):
                    if hasattr(widget, 'refresh_stats'):
                        widget.refresh_stats()
                    elif hasattr(widget, 'refresh_sync'):
                        widget.refresh_sync()
                    elif hasattr(widget, 'refresh_alerts'):
                        widget.refresh_alerts()
                    elif hasattr(widget, 'refresh_vendors'):
                        widget.refresh_vendors()
        except Exception:
            pass
        self.notify("Refreshed!", severity="information")

    @work(thread=True)
    def action_sync(self) -> None:
        """Run data sync in background."""
        self.notify("Starting sync...", severity="information")
        from fraudit.ingestion import run_sync
        try:
            results = run_sync()
            total = sum(r.get("records", 0) for r in results.values() if r.get("status") == "success")
            self.call_from_thread(self.notify, f"Sync complete: {total:,} records", severity="information")
            self.call_from_thread(self.action_refresh)
        except Exception as e:
            self.call_from_thread(self.notify, f"Sync error: {e}", severity="error")


def run():
    """Run the TUI application."""
    app = FrauditApp()
    app.run()


if __name__ == "__main__":
    run()
