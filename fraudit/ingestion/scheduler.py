"""
Scheduler for automatic data synchronization.

Uses APScheduler to run sync jobs at configured intervals.
"""

import signal
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from fraudit.config import config


def sync_job():
    """Run all data syncs."""
    from fraudit.ingestion import run_sync

    print(f"\n[{datetime.now()}] Starting scheduled sync...")
    try:
        results = run_sync()
        for source, result in results.items():
            status = result.get("status", "unknown")
            records = result.get("records", 0)
            print(f"  {source}: {status} ({records} records)")
    except Exception as e:
        print(f"  Sync error: {e}")

    print(f"[{datetime.now()}] Sync complete.\n")


def analysis_job():
    """Run fraud detection analysis."""
    from fraudit.detection import run_detection

    print(f"\n[{datetime.now()}] Starting scheduled analysis...")
    try:
        alerts = run_detection()
        print(f"  Generated {alerts} new alerts")
    except Exception as e:
        print(f"  Analysis error: {e}")

    print(f"[{datetime.now()}] Analysis complete.\n")


def check_pia_deadlines():
    """Check for overdue PIA requests."""
    from fraudit.pia import PIAManager

    manager = PIAManager()
    overdue = manager.check_overdue()

    if overdue:
        print(f"\n[{datetime.now()}] {len(overdue)} PIA request(s) are now overdue!")
        # Could trigger notifications here


def start_scheduler(foreground: bool = True):
    """
    Start the scheduler.

    Args:
        foreground: If True, run in blocking mode. Otherwise, background.
    """
    if foreground:
        scheduler = BlockingScheduler()
    else:
        scheduler = BackgroundScheduler()

    # Get sync interval from config
    interval_hours = config.sync_interval_hours

    # Schedule sync job
    scheduler.add_job(
        sync_job,
        trigger=IntervalTrigger(hours=interval_hours),
        id="sync_job",
        name="Data synchronization",
        max_instances=1,
        coalesce=True,
    )

    # Schedule analysis job (run after sync)
    scheduler.add_job(
        analysis_job,
        trigger=IntervalTrigger(hours=interval_hours, minutes=30),
        id="analysis_job",
        name="Fraud detection analysis",
        max_instances=1,
        coalesce=True,
    )

    # Check PIA deadlines daily
    scheduler.add_job(
        check_pia_deadlines,
        trigger=IntervalTrigger(hours=24),
        id="pia_deadline_check",
        name="PIA deadline check",
        max_instances=1,
    )

    # Handle shutdown gracefully
    def shutdown(signum, frame):
        print("\nShutting down scheduler...")
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Scheduler started. Sync interval: {interval_hours} hours")
    print("Jobs:")
    for job in scheduler.get_jobs():
        print(f"  - {job.name}: {job.trigger}")

    # Run initial sync
    print("\nRunning initial sync...")
    sync_job()
    analysis_job()

    if foreground:
        print("\nScheduler running. Press Ctrl+C to stop.")
        scheduler.start()
    else:
        scheduler.start()
        return scheduler
