"""Alert management functionality."""

from typing import Optional

from fraudit.database import get_session, Alert, AlertSeverity, AlertStatus


class AlertManager:
    """Manages alert creation and lifecycle."""

    @staticmethod
    def create(
        alert_type: str,
        severity: str,
        title: str,
        description: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[int] = None,
        evidence: Optional[dict] = None,
    ) -> Alert:
        """
        Create a new alert.

        Args:
            alert_type: Type of alert (e.g., 'contract_splitting')
            severity: Alert severity ('low', 'medium', 'high')
            title: Short alert title
            description: Detailed description
            entity_type: Type of related entity ('vendor', 'contract', etc.)
            entity_id: ID of related entity
            evidence: Supporting data as dict

        Returns:
            Created Alert object.
        """
        with get_session() as session:
            alert = Alert(
                alert_type=alert_type,
                severity=AlertSeverity(severity),
                title=title,
                description=description,
                entity_type=entity_type,
                entity_id=entity_id,
                evidence=evidence,
                status=AlertStatus.NEW,
            )
            session.add(alert)
            session.flush()
            alert_id = alert.id

        return alert_id

    @staticmethod
    def check_duplicate(
        alert_type: str,
        entity_type: str,
        entity_id: int,
    ) -> bool:
        """
        Check if a similar alert already exists.

        Returns True if a non-resolved alert exists for same entity.
        """
        with get_session() as session:
            existing = session.query(Alert).filter(
                Alert.alert_type == alert_type,
                Alert.entity_type == entity_type,
                Alert.entity_id == entity_id,
                Alert.status.not_in([AlertStatus.RESOLVED, AlertStatus.FALSE_POSITIVE]),
            ).first()

            return existing is not None


def create_alert(
    alert_type: str,
    severity: str,
    title: str,
    description: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    evidence: Optional[dict] = None,
    skip_duplicate_check: bool = False,
) -> Optional[int]:
    """
    Convenience function to create an alert.

    Returns alert ID if created, None if duplicate exists.
    """
    manager = AlertManager()

    if not skip_duplicate_check and entity_type and entity_id:
        if manager.check_duplicate(alert_type, entity_type, entity_id):
            return None

    return manager.create(
        alert_type=alert_type,
        severity=severity,
        title=title,
        description=description,
        entity_type=entity_type,
        entity_id=entity_id,
        evidence=evidence,
    )
