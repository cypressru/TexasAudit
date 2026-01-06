"""Base class for data ingestors."""

from abc import ABC, abstractmethod
from datetime import datetime

from fraudit.database import get_session, SyncStatus, SyncStatusEnum


class BaseIngestor(ABC):
    """Base class for all data ingestors."""

    source_name: str = "base"

    def __init__(self):
        self.sync_id = None

    def _start_sync(self) -> int:
        """Record sync start in database."""
        with get_session() as session:
            status = SyncStatus(
                source_name=self.source_name,
                status=SyncStatusEnum.IN_PROGRESS,
            )
            session.add(status)
            session.flush()
            self.sync_id = status.id
            return status.id

    def _complete_sync(self, records_synced: int, error: str | None = None):
        """Record sync completion in database."""
        with get_session() as session:
            status = session.get(SyncStatus, self.sync_id)
            if status:
                status.completed_at = datetime.now()
                status.records_synced = records_synced
                status.status = SyncStatusEnum.FAILED if error else SyncStatusEnum.SUCCESS
                status.error_message = error

    def _get_last_sync(self) -> datetime | None:
        """Get timestamp of last successful sync."""
        with get_session() as session:
            status = session.query(SyncStatus).filter(
                SyncStatus.source_name == self.source_name,
                SyncStatus.status == SyncStatusEnum.SUCCESS,
            ).order_by(SyncStatus.completed_at.desc()).first()

            return status.completed_at if status else None

    def sync(self, full: bool = False) -> int:
        """
        Run the sync operation.

        Args:
            full: If True, ignore last sync timestamp.

        Returns:
            Number of records synced.
        """
        self._start_sync()

        try:
            last_sync = None if full else self._get_last_sync()
            count = self._do_sync(since=last_sync)
            self._complete_sync(count)
            return count
        except Exception as e:
            self._complete_sync(0, str(e))
            raise

    @abstractmethod
    def _do_sync(self, since: datetime | None = None) -> int:
        """
        Perform the actual sync operation.

        Args:
            since: Only sync records updated after this timestamp.

        Returns:
            Number of records synced.
        """
        pass
