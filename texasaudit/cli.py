"""
Texas Audit CLI - Command Line Interface

Entry point for all Texas Audit operations.
"""

import click
from tabulate import tabulate

from texasaudit import __version__
from texasaudit.config import config


@click.group()
@click.version_option(version=__version__, prog_name="texasaudit")
@click.pass_context
def cli(ctx):
    """Texas Audit - Government Spending Fraud Detection System.

    A comprehensive tool for monitoring Texas state government spending,
    detecting potential fraud patterns, and managing public information requests.
    """
    ctx.ensure_object(dict)


# =============================================================================
# Init & Config Commands
# =============================================================================

@cli.command()
@click.option("--drop", is_flag=True, help="Drop existing tables before creating")
@click.pass_context
def init(ctx, drop):
    """Initialize the database and create all tables."""
    from texasaudit.database import init_db, drop_db, get_engine

    click.echo("Initializing Texas Audit database...")

    try:
        # Test connection
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(click.style("SELECT 1", fg="green"))
        click.echo(f"  Connected to: {config.database_url.split('@')[1]}")
    except Exception as e:
        click.echo(click.style(f"  Database connection failed: {e}", fg="red"))
        click.echo("\nCheck your config.yaml database settings or environment variables.")
        raise SystemExit(1)

    if drop:
        if click.confirm("This will DELETE all existing data. Continue?"):
            click.echo("  Dropping existing tables...")
            drop_db()
        else:
            click.echo("Aborted.")
            return

    click.echo("  Creating tables...")
    init_db()
    click.echo(click.style("Database initialized successfully!", fg="green"))


@cli.command("config")
@click.option("--show", is_flag=True, help="Show current configuration")
def show_config(show):
    """View or edit configuration."""
    if show:
        import os
        click.echo("\n=== Current Configuration ===\n")
        click.echo(f"Database URL: {config.database_url.split('@')[1] if '@' in config.database_url else config.database_url}")
        click.echo(f"Socrata Token: {'[SET]' if config.socrata_token else '[NOT SET]'}")
        click.echo(f"SAM.gov API Key: {'[SET]' if os.environ.get('SAM_API_KEY') else '[NOT SET]'}")
        click.echo(f"Sync Interval: {config.sync_interval_hours} hours")
        click.echo(f"Data Directory: {config.data_dir}")
        click.echo(f"\nDetection Thresholds:")
        for key, value in config.detection_thresholds.items():
            click.echo(f"  {key}: {value}")
    else:
        click.echo("Edit config.yaml directly or set environment variables.")
        click.echo("Use 'texasaudit config --show' to view current settings.")


# =============================================================================
# Sync Commands
# =============================================================================

@cli.group()
def sync():
    """Data synchronization commands."""
    pass


@sync.command("run")
@click.option("--source", "-s", help="Sync specific source only")
@click.option("--full", is_flag=True, help="Full sync (ignore last sync timestamp)")
@click.option("--all", "sync_all", is_flag=True, help="Sync all sources even if already synced")
def sync_run(source, full, sync_all):
    """Run data synchronization with live progress display."""
    from texasaudit.ingestion.runner import run_sync

    sources = [source] if source else None
    smart = not sync_all

    run_sync(sources=sources, smart=smart)


@sync.command("status")
def sync_status():
    """Show sync status for all sources."""
    from texasaudit.database import get_session, SyncStatus

    with get_session() as session:
        statuses = session.query(SyncStatus).order_by(
            SyncStatus.source_name, SyncStatus.started_at.desc()
        ).all()

        if not statuses:
            click.echo("No sync history found. Run 'texasaudit sync run' to start.")
            return

        # Group by source, show latest
        latest = {}
        for s in statuses:
            if s.source_name not in latest:
                latest[s.source_name] = s

        rows = []
        for name, s in latest.items():
            status_color = {
                "success": "green",
                "failed": "red",
                "in_progress": "yellow",
            }.get(s.status.value, "white")

            rows.append([
                name,
                click.style(s.status.value, fg=status_color),
                s.records_synced,
                s.started_at.strftime("%Y-%m-%d %H:%M"),
                s.error_message[:50] if s.error_message else "-",
            ])

        click.echo(tabulate(
            rows,
            headers=["Source", "Status", "Records", "Last Run", "Error"],
            tablefmt="simple",
        ))


# =============================================================================
# Analysis Commands
# =============================================================================

@cli.group()
def analyze():
    """Fraud detection and analysis commands."""
    pass


@analyze.command("run")
@click.option("--rule", "-r", help="Run specific detection rule only")
@click.option("--vendor", "-v", help="Analyze specific vendor ID")
def analyze_run(rule, vendor):
    """Run fraud detection analysis."""
    from texasaudit.detection import run_detection

    click.echo("Running fraud detection analysis...")
    alerts_created = run_detection(rule=rule, vendor_id=vendor)
    click.echo(f"\nAnalysis complete. {alerts_created} new alerts generated.")


@analyze.command("rules")
def analyze_rules():
    """List available detection rules."""
    rules = [
        ("contract-splitting", "Detect contracts clustered below reporting thresholds"),
        ("duplicate-payments", "Find duplicate payment transactions"),
        ("vendor-clustering", "Identify related vendors by name/address"),
        ("network-analysis", "Analyze vendor-agency relationship networks"),
        ("payment-anomalies", "Detect unusual payment patterns"),
        ("confidentiality", "Flag unusual confidentiality rates"),
        ("ghost-vendors", "Find vendors receiving payments but not in CMBL"),
        ("employee-vendor", "Detect employee names matching vendor owners"),
        ("fiscal-year-rush", "Flag year-end spending spikes"),
        ("related-party", "Identify related party transactions"),
        ("debarment", "Screen vendors against SAM.gov exclusions"),
        ("pay-to-play", "Correlate campaign contributions with contracts"),
    ]

    click.echo("\n=== Available Detection Rules ===\n")
    for name, desc in rules:
        click.echo(f"  {name:25} {desc}")


# =============================================================================
# Alert Commands
# =============================================================================

@cli.group()
def alerts():
    """Alert management commands."""
    pass


@alerts.command("list")
@click.option("--severity", "-s", type=click.Choice(["low", "medium", "high"]))
@click.option("--status", type=click.Choice(["new", "acknowledged", "investigating", "resolved", "false_positive"]))
@click.option("--limit", "-n", default=20, help="Number of alerts to show")
def alerts_list(severity, status, limit):
    """List alerts."""
    from texasaudit.database import get_session, Alert, AlertSeverity, AlertStatus

    with get_session() as session:
        query = session.query(Alert).order_by(Alert.created_at.desc())

        if severity:
            query = query.filter(Alert.severity == AlertSeverity(severity))
        if status:
            query = query.filter(Alert.status == AlertStatus(status))

        alerts = query.limit(limit).all()

        if not alerts:
            click.echo("No alerts found.")
            return

        rows = []
        for a in alerts:
            severity_color = {
                "high": "red",
                "medium": "yellow",
                "low": "cyan",
            }.get(a.severity.value, "white")

            rows.append([
                a.id,
                click.style(a.severity.value.upper(), fg=severity_color),
                a.status.value,
                a.title[:50],
                a.created_at.strftime("%Y-%m-%d"),
            ])

        click.echo(tabulate(
            rows,
            headers=["ID", "Severity", "Status", "Title", "Created"],
            tablefmt="simple",
        ))


@alerts.command("show")
@click.argument("alert_id", type=int)
def alerts_show(alert_id):
    """Show alert details."""
    from texasaudit.database import get_session, Alert

    with get_session() as session:
        alert = session.get(Alert, alert_id)

        if not alert:
            click.echo(f"Alert {alert_id} not found.")
            return

        click.echo(f"\n=== Alert #{alert.id} ===\n")
        click.echo(f"Title:    {alert.title}")
        click.echo(f"Severity: {alert.severity.value}")
        click.echo(f"Status:   {alert.status.value}")
        click.echo(f"Type:     {alert.alert_type}")
        click.echo(f"Entity:   {alert.entity_type} #{alert.entity_id}")
        click.echo(f"Created:  {alert.created_at}")
        click.echo(f"\nDescription:\n{alert.description}")

        if alert.evidence:
            click.echo(f"\nEvidence:\n{alert.evidence}")

        if alert.notes:
            click.echo(f"\nNotes:\n{alert.notes}")


@alerts.command("acknowledge")
@click.argument("alert_id", type=int)
def alerts_acknowledge(alert_id):
    """Acknowledge an alert."""
    from datetime import datetime
    from texasaudit.database import get_session, Alert, AlertStatus

    with get_session() as session:
        alert = session.get(Alert, alert_id)
        if not alert:
            click.echo(f"Alert {alert_id} not found.")
            return

        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.now()
        session.commit()
        click.echo(f"Alert {alert_id} acknowledged.")


@alerts.command("resolve")
@click.argument("alert_id", type=int)
@click.option("--note", "-n", help="Resolution note")
@click.option("--false-positive", is_flag=True, help="Mark as false positive")
def alerts_resolve(alert_id, note, false_positive):
    """Resolve an alert."""
    from datetime import datetime
    from texasaudit.database import get_session, Alert, AlertStatus

    with get_session() as session:
        alert = session.get(Alert, alert_id)
        if not alert:
            click.echo(f"Alert {alert_id} not found.")
            return

        alert.status = AlertStatus.FALSE_POSITIVE if false_positive else AlertStatus.RESOLVED
        alert.resolved_at = datetime.now()
        if note:
            alert.notes = (alert.notes or "") + f"\n[{datetime.now()}] {note}"
        session.commit()
        click.echo(f"Alert {alert_id} resolved.")


# =============================================================================
# Vendor Commands
# =============================================================================

@cli.group()
def vendors():
    """Vendor lookup and analysis commands."""
    pass


@vendors.command("search")
@click.argument("query")
@click.option("--limit", "-n", default=20)
def vendors_search(query, limit):
    """Search for vendors by name."""
    from texasaudit.database import get_session, Vendor

    with get_session() as session:
        results = session.query(Vendor).filter(
            Vendor.name.ilike(f"%{query}%")
        ).limit(limit).all()

        if not results:
            click.echo(f"No vendors found matching '{query}'")
            return

        rows = [[v.vendor_id, v.name[:50], v.city, v.state, v.risk_score or "-"] for v in results]
        click.echo(tabulate(
            rows,
            headers=["VID", "Name", "City", "State", "Risk"],
            tablefmt="simple",
        ))


@vendors.command("show")
@click.argument("vendor_id")
def vendors_show(vendor_id):
    """Show vendor details."""
    from texasaudit.database import get_session, Vendor

    with get_session() as session:
        vendor = session.query(Vendor).filter(
            (Vendor.vendor_id == vendor_id) | (Vendor.id == vendor_id)
        ).first()

        if not vendor:
            click.echo(f"Vendor {vendor_id} not found.")
            return

        click.echo(f"\n=== Vendor: {vendor.name} ===\n")
        click.echo(f"VID:        {vendor.vendor_id}")
        click.echo(f"Address:    {vendor.address}")
        click.echo(f"City/State: {vendor.city}, {vendor.state} {vendor.zip_code}")
        click.echo(f"HUB Status: {vendor.hub_status or 'N/A'}")
        click.echo(f"In CMBL:    {vendor.in_cmbl}")
        click.echo(f"Risk Score: {vendor.risk_score or 'Not calculated'}")
        click.echo(f"First Seen: {vendor.first_seen}")
        click.echo(f"Last Seen:  {vendor.last_seen}")

        # Show payment summary
        total_payments = sum(p.amount for p in vendor.payments)
        click.echo(f"\nPayments:   {len(vendor.payments)} totaling ${total_payments:,.2f}")
        click.echo(f"Contracts:  {len(vendor.contracts)}")


@vendors.command("related")
@click.argument("vendor_id")
def vendors_related(vendor_id):
    """Show vendors related to this one."""
    from texasaudit.database import get_session, Vendor, VendorRelationship

    with get_session() as session:
        vendor = session.query(Vendor).filter(
            (Vendor.vendor_id == vendor_id) | (Vendor.id == vendor_id)
        ).first()

        if not vendor:
            click.echo(f"Vendor {vendor_id} not found.")
            return

        # Get relationships
        relationships = session.query(VendorRelationship).filter(
            (VendorRelationship.vendor_id_1 == vendor.id) |
            (VendorRelationship.vendor_id_2 == vendor.id)
        ).all()

        if not relationships:
            click.echo(f"No related vendors found for {vendor.name}")
            return

        click.echo(f"\n=== Vendors Related to: {vendor.name} ===\n")
        rows = []
        for r in relationships:
            other_id = r.vendor_id_2 if r.vendor_id_1 == vendor.id else r.vendor_id_1
            other = session.get(Vendor, other_id)
            rows.append([
                other.vendor_id,
                other.name[:40],
                r.relationship_type,
                f"{float(r.confidence_score):.2f}" if r.confidence_score else "-",
            ])

        click.echo(tabulate(
            rows,
            headers=["VID", "Name", "Relationship", "Confidence"],
            tablefmt="simple",
        ))


# =============================================================================
# PIA Request Commands
# =============================================================================

@cli.group()
def pia():
    """Public Information Act request management."""
    pass


@pia.command("list")
@click.option("--status", type=click.Choice(["draft", "submitted", "pending", "received", "overdue", "closed"]))
def pia_list(status):
    """List PIA requests."""
    from texasaudit.database import get_session, PIARequest, PIAStatus

    with get_session() as session:
        query = session.query(PIARequest).order_by(PIARequest.created_at.desc())

        if status:
            query = query.filter(PIARequest.status == PIAStatus(status))

        requests = query.all()

        if not requests:
            click.echo("No PIA requests found.")
            return

        rows = []
        for r in requests:
            status_color = {
                "overdue": "red",
                "pending": "yellow",
                "received": "green",
            }.get(r.status.value, "white")

            rows.append([
                r.id,
                click.style(r.status.value, fg=status_color),
                r.subject[:40],
                r.submitted_date or "-",
                r.due_date or "-",
            ])

        click.echo(tabulate(
            rows,
            headers=["ID", "Status", "Subject", "Submitted", "Due"],
            tablefmt="simple",
        ))


@pia.command("draft")
@click.option("--alert", "-a", type=int, help="Create from alert ID")
@click.option("--agency", help="Agency code")
@click.option("--subject", "-s", help="Request subject")
def pia_draft(alert, agency, subject):
    """Draft a new PIA request."""
    from texasaudit.pia import create_draft

    if alert:
        click.echo(f"Creating PIA request from alert #{alert}...")
        request_id = create_draft(alert_id=alert)
    else:
        if not agency or not subject:
            click.echo("Provide --agency and --subject, or use --alert to create from an alert.")
            return
        request_id = create_draft(agency_code=agency, subject=subject)

    click.echo(f"Draft PIA request #{request_id} created.")
    click.echo("Edit with 'texasaudit pia edit {request_id}' or submit with 'texasaudit pia submit {request_id}'")


@pia.command("show")
@click.argument("request_id", type=int)
def pia_show(request_id):
    """Show PIA request details."""
    from texasaudit.database import get_session, PIARequest

    with get_session() as session:
        request = session.get(PIARequest, request_id)

        if not request:
            click.echo(f"PIA request {request_id} not found.")
            return

        click.echo(f"\n=== PIA Request #{request.id} ===\n")
        click.echo(f"Subject:   {request.subject}")
        click.echo(f"Agency:    {request.agency.name if request.agency else 'Not set'}")
        click.echo(f"Status:    {request.status.value}")
        click.echo(f"Submitted: {request.submitted_date or 'Not submitted'}")
        click.echo(f"Due Date:  {request.due_date or 'N/A'}")
        click.echo(f"\n--- Request Text ---\n{request.request_text}")

        if request.response_notes:
            click.echo(f"\n--- Response Notes ---\n{request.response_notes}")


# =============================================================================
# Terminal UI Command
# =============================================================================

@cli.command()
def tui():
    """Start the terminal UI dashboard."""
    from texasaudit.tui import TexasAuditApp

    app = TexasAuditApp()
    app.run()


# =============================================================================
# Scheduler Command
# =============================================================================

@cli.command()
@click.option("--foreground", "-f", is_flag=True, help="Run in foreground")
def scheduler(foreground):
    """Start the background scheduler for automatic syncs."""
    from texasaudit.ingestion.scheduler import start_scheduler

    click.echo("Starting scheduler...")
    click.echo(f"Sync interval: {config.sync_interval_hours} hours")

    start_scheduler(foreground=foreground)


if __name__ == "__main__":
    cli()
