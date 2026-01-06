"""SQLAlchemy models for Texas Audit database."""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    String,
    Text,
    Integer,
    BigInteger,
    Numeric,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Enum,
    JSON,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class AlertSeverity(PyEnum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AlertStatus(PyEnum):
    """Alert status workflow."""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    FALSE_POSITIVE = "false_positive"


class PIAStatus(PyEnum):
    """PIA request status."""
    DRAFT = "draft"
    SUBMITTED = "submitted"
    PENDING = "pending"
    RECEIVED = "received"
    OVERDUE = "overdue"
    CLOSED = "closed"


class SyncStatusEnum(PyEnum):
    """Sync job status."""
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


class Agency(Base):
    """Texas state agency."""
    __tablename__ = "agencies"

    id: Mapped[int] = mapped_column(primary_key=True)
    agency_code: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    category: Mapped[Optional[str]] = mapped_column(String(100))
    confidentiality_rate: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), comment="Computed rate of confidential transactions"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    payments: Mapped[list["Payment"]] = relationship(back_populates="agency")
    contracts: Mapped[list["Contract"]] = relationship(back_populates="agency")
    grants: Mapped[list["Grant"]] = relationship(back_populates="agency")
    pia_requests: Mapped[list["PIARequest"]] = relationship(back_populates="agency")
    employees: Mapped[list["Employee"]] = relationship(back_populates="agency")

    def __repr__(self) -> str:
        return f"<Agency {self.agency_code}: {self.name}>"


class Vendor(Base):
    """Vendor/payee in the Texas procurement system."""
    __tablename__ = "vendors"

    id: Mapped[int] = mapped_column(primary_key=True)
    vendor_id: Mapped[Optional[str]] = mapped_column(
        String(50), unique=True, index=True, comment="State-assigned VID"
    )
    name: Mapped[str] = mapped_column(String(500), index=True)
    name_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    address: Mapped[Optional[str]] = mapped_column(String(500))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    zip_code: Mapped[Optional[str]] = mapped_column(String(20))
    phone: Mapped[Optional[str]] = mapped_column(String(50))
    hub_status: Mapped[Optional[str]] = mapped_column(
        String(50), comment="HUB certification status"
    )
    nigp_codes: Mapped[Optional[list]] = mapped_column(
        ARRAY(String(20)), comment="NIGP commodity codes"
    )
    first_seen: Mapped[Optional[date]] = mapped_column(Date)
    last_seen: Mapped[Optional[date]] = mapped_column(Date)
    risk_score: Mapped[Optional[int]] = mapped_column(
        Integer, comment="Computed risk score 0-100"
    )
    in_cmbl: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Whether vendor is in CMBL"
    )
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    payments: Mapped[list["Payment"]] = relationship(back_populates="vendor")
    contracts: Mapped[list["Contract"]] = relationship(back_populates="vendor")
    grants: Mapped[list["Grant"]] = relationship(back_populates="recipient")
    related_from: Mapped[list["VendorRelationship"]] = relationship(
        back_populates="vendor_1",
        foreign_keys="VendorRelationship.vendor_id_1"
    )
    related_to: Mapped[list["VendorRelationship"]] = relationship(
        back_populates="vendor_2",
        foreign_keys="VendorRelationship.vendor_id_2"
    )

    __table_args__ = (
        Index("ix_vendors_name_normalized_gin", "name_normalized"),
        Index("ix_vendors_address", "address"),
        Index("ix_vendors_city_state", "city", "state"),
    )

    def __repr__(self) -> str:
        return f"<Vendor {self.vendor_id}: {self.name}>"


class Payment(Base):
    """Individual payment transaction."""
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vendor_id: Mapped[Optional[int]] = mapped_column(ForeignKey("vendors.id"), index=True)
    agency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("agencies.id"), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), index=True)
    payment_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    fiscal_year_state: Mapped[Optional[int]] = mapped_column(
        Integer, index=True, comment="Texas FY (Sep 1 - Aug 31)"
    )
    fiscal_year_federal: Mapped[Optional[int]] = mapped_column(
        Integer, comment="Federal FY (Oct 1 - Sep 30)"
    )
    calendar_year: Mapped[Optional[int]] = mapped_column(Integer)
    comptroller_object_code: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    is_confidential: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    source_system: Mapped[Optional[str]] = mapped_column(
        String(50), comment="Origin: comptroller, socrata, etc."
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        String(100), comment="Original ID from source system"
    )
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    # Relationships
    vendor: Mapped[Optional["Vendor"]] = relationship(back_populates="payments")
    agency: Mapped[Optional["Agency"]] = relationship(back_populates="payments")

    __table_args__ = (
        Index("ix_payments_vendor_date", "vendor_id", "payment_date"),
        Index("ix_payments_agency_date", "agency_id", "payment_date"),
        Index("ix_payments_fy_amount", "fiscal_year_state", "amount"),
    )

    def __repr__(self) -> str:
        return f"<Payment ${self.amount} to vendor {self.vendor_id}>"


class Contract(Base):
    """Government contract."""
    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(primary_key=True)
    contract_number: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    vendor_id: Mapped[Optional[int]] = mapped_column(ForeignKey("vendors.id"), index=True)
    agency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("agencies.id"), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    current_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), index=True)
    max_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    start_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    nigp_codes: Mapped[Optional[list]] = mapped_column(ARRAY(String(20)))
    source: Mapped[Optional[str]] = mapped_column(
        String(50), comment="LBB, TxSmartBuy, DIR, etc."
    )
    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    vendor: Mapped[Optional["Vendor"]] = relationship(back_populates="contracts")
    agency: Mapped[Optional["Agency"]] = relationship(back_populates="contracts")

    __table_args__ = (
        Index("ix_contracts_vendor_value", "vendor_id", "current_value"),
        Index("ix_contracts_agency_value", "agency_id", "current_value"),
    )

    def __repr__(self) -> str:
        return f"<Contract {self.contract_number}>"


class Grant(Base):
    """Grant award."""
    __tablename__ = "grants"

    id: Mapped[int] = mapped_column(primary_key=True)
    grant_number: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    recipient_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("vendors.id"), index=True, comment="FK to vendors for consistency"
    )
    agency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("agencies.id"), index=True)
    program_name: Mapped[Optional[str]] = mapped_column(String(500))
    amount_awarded: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    amount_disbursed: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    fiscal_year: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    start_date: Mapped[Optional[date]] = mapped_column(Date)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    source: Mapped[Optional[str]] = mapped_column(
        String(50), comment="state, federal_passthrough, usaspending"
    )
    federal_award_id: Mapped[Optional[str]] = mapped_column(
        String(100), comment="USASpending award ID if federal"
    )
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    # Relationships
    recipient: Mapped[Optional["Vendor"]] = relationship(back_populates="grants")
    agency: Mapped[Optional["Agency"]] = relationship(back_populates="grants")

    def __repr__(self) -> str:
        return f"<Grant {self.grant_number}>"


class Alert(Base):
    """Fraud detection alert."""
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(primary_key=True)
    alert_type: Mapped[str] = mapped_column(String(100), index=True)
    severity: Mapped[AlertSeverity] = mapped_column(
        Enum(AlertSeverity), index=True
    )
    title: Mapped[str] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text)
    entity_type: Mapped[Optional[str]] = mapped_column(
        String(50), comment="vendor, contract, payment, agency"
    )
    entity_id: Mapped[Optional[int]] = mapped_column(
        Integer, comment="ID of related entity"
    )
    evidence: Mapped[Optional[dict]] = mapped_column(
        JSONB, comment="Supporting data for the alert"
    )
    status: Mapped[AlertStatus] = mapped_column(
        Enum(AlertStatus), default=AlertStatus.NEW, index=True
    )
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    pia_requests: Mapped[list["PIARequest"]] = relationship(back_populates="related_alert")

    __table_args__ = (
        Index("ix_alerts_status_severity", "status", "severity"),
        Index("ix_alerts_entity", "entity_type", "entity_id"),
    )

    def __repr__(self) -> str:
        return f"<Alert [{self.severity.value}] {self.title}>"


class PIARequest(Base):
    """Public Information Act request tracker."""
    __tablename__ = "pia_requests"

    id: Mapped[int] = mapped_column(primary_key=True)
    agency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("agencies.id"), index=True)
    subject: Mapped[str] = mapped_column(String(500))
    request_text: Mapped[str] = mapped_column(Text)
    related_alert_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("alerts.id"), index=True
    )
    status: Mapped[PIAStatus] = mapped_column(
        Enum(PIAStatus), default=PIAStatus.DRAFT, index=True
    )
    submitted_date: Mapped[Optional[date]] = mapped_column(Date)
    due_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    received_date: Mapped[Optional[date]] = mapped_column(Date)
    response_notes: Mapped[Optional[str]] = mapped_column(Text)
    attachments_path: Mapped[Optional[str]] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    agency: Mapped[Optional["Agency"]] = relationship(back_populates="pia_requests")
    related_alert: Mapped[Optional["Alert"]] = relationship(back_populates="pia_requests")

    def __repr__(self) -> str:
        return f"<PIARequest #{self.id}: {self.subject}>"


class SyncStatus(Base):
    """Data sync job status tracking."""
    __tablename__ = "sync_status"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_name: Mapped[str] = mapped_column(String(100), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    records_synced: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[SyncStatusEnum] = mapped_column(
        Enum(SyncStatusEnum), default=SyncStatusEnum.IN_PROGRESS
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    details: Mapped[Optional[dict]] = mapped_column(JSONB)

    def __repr__(self) -> str:
        return f"<SyncStatus {self.source_name}: {self.status.value}>"


class VendorRelationship(Base):
    """Detected relationships between vendors."""
    __tablename__ = "vendor_relationships"

    id: Mapped[int] = mapped_column(primary_key=True)
    vendor_id_1: Mapped[int] = mapped_column(ForeignKey("vendors.id"), index=True)
    vendor_id_2: Mapped[int] = mapped_column(ForeignKey("vendors.id"), index=True)
    relationship_type: Mapped[str] = mapped_column(
        String(50), index=True,
        comment="same_address, similar_name, shared_principal, common_contracts"
    )
    confidence_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), comment="0-1 confidence in relationship"
    )
    evidence: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    # Relationships
    vendor_1: Mapped["Vendor"] = relationship(
        back_populates="related_from",
        foreign_keys=[vendor_id_1]
    )
    vendor_2: Mapped["Vendor"] = relationship(
        back_populates="related_to",
        foreign_keys=[vendor_id_2]
    )

    __table_args__ = (
        Index("ix_vendor_relationships_pair", "vendor_id_1", "vendor_id_2"),
    )

    def __repr__(self) -> str:
        return f"<VendorRelationship {self.vendor_id_1} <-> {self.vendor_id_2}: {self.relationship_type}>"


class Employee(Base):
    """State employee salary data."""
    __tablename__ = "employees"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(500), index=True)
    name_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    agency_id: Mapped[Optional[int]] = mapped_column(ForeignKey("agencies.id"), index=True)
    job_title: Mapped[Optional[str]] = mapped_column(String(500))
    annual_salary: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), index=True)
    hire_date: Mapped[Optional[date]] = mapped_column(Date)
    employment_status: Mapped[Optional[str]] = mapped_column(
        String(50), comment="active, terminated, retired, etc."
    )
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    agency: Mapped[Optional["Agency"]] = relationship(back_populates="employees")

    __table_args__ = (
        Index("ix_employees_agency_salary", "agency_id", "annual_salary"),
    )

    def __repr__(self) -> str:
        return f"<Employee {self.name}: {self.job_title}>"


class CampaignContribution(Base):
    """Campaign finance data."""
    __tablename__ = "campaign_contributions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    filer_name: Mapped[str] = mapped_column(String(500), index=True)
    filer_type: Mapped[Optional[str]] = mapped_column(
        String(50), comment="candidate, pac, political_committee, etc."
    )
    contributor_name: Mapped[str] = mapped_column(String(500), index=True)
    contributor_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    contributor_type: Mapped[Optional[str]] = mapped_column(
        String(50), comment="individual, corporation, pac, etc."
    )
    contribution_amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), index=True)
    contribution_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    contributor_city: Mapped[Optional[str]] = mapped_column(String(100))
    contributor_state: Mapped[Optional[str]] = mapped_column(String(2))
    contributor_employer: Mapped[Optional[str]] = mapped_column(String(500))
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_campaign_contributions_filer_date", "filer_name", "contribution_date"),
    )

    def __repr__(self) -> str:
        return f"<CampaignContribution ${self.contribution_amount} from {self.contributor_name} to {self.filer_name}>"


class LobbyingActivity(Base):
    """Lobbying registrations and activities."""
    __tablename__ = "lobbying_activities"

    id: Mapped[int] = mapped_column(primary_key=True)
    registrant_name: Mapped[str] = mapped_column(String(500), index=True)
    registrant_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    client_name: Mapped[str] = mapped_column(String(500), index=True)
    client_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    subject_matter: Mapped[Optional[str]] = mapped_column(Text)
    compensation_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), index=True)
    registration_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    termination_date: Mapped[Optional[date]] = mapped_column(Date)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    # Indexes defined via index=True on columns

    def __repr__(self) -> str:
        return f"<LobbyingActivity {self.registrant_name} for {self.client_name}>"


class TaxPermit(Base):
    """Business tax permits (franchise/sales)."""
    __tablename__ = "tax_permits"

    id: Mapped[int] = mapped_column(primary_key=True)
    permit_type: Mapped[str] = mapped_column(
        String(50), index=True, comment="franchise, sales_tax, etc."
    )
    taxpayer_name: Mapped[str] = mapped_column(String(500), index=True)
    taxpayer_normalized: Mapped[Optional[str]] = mapped_column(
        String(500), index=True, comment="Standardized name for matching"
    )
    taxpayer_number: Mapped[Optional[str]] = mapped_column(
        String(50), index=True, comment="State tax ID"
    )
    business_name: Mapped[Optional[str]] = mapped_column(String(500), index=True)
    business_address: Mapped[Optional[str]] = mapped_column(String(500))
    business_city: Mapped[Optional[str]] = mapped_column(String(100))
    business_state: Mapped[Optional[str]] = mapped_column(String(2))
    business_zip: Mapped[Optional[str]] = mapped_column(String(20))
    naics_code: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    sic_code: Mapped[Optional[str]] = mapped_column(String(20))
    permit_status: Mapped[Optional[str]] = mapped_column(
        String(50), comment="active, inactive, suspended, etc."
    )
    first_sale_date: Mapped[Optional[date]] = mapped_column(Date)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    # Indexes defined via index=True on columns

    def __repr__(self) -> str:
        return f"<TaxPermit {self.permit_type}: {self.taxpayer_name}>"


class EntityMatch(Base):
    """Cross-reference matches between entities."""
    __tablename__ = "entity_matches"

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type_1: Mapped[str] = mapped_column(
        String(50), index=True, comment="vendor, employee, contributor, lobbyist, taxpayer"
    )
    entity_id_1: Mapped[int] = mapped_column(Integer, index=True)
    entity_type_2: Mapped[str] = mapped_column(
        String(50), index=True, comment="vendor, employee, contributor, lobbyist, taxpayer"
    )
    entity_id_2: Mapped[int] = mapped_column(Integer, index=True)
    match_type: Mapped[str] = mapped_column(
        String(50), index=True, comment="name, address, employer, tax_id, etc."
    )
    confidence_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4), comment="0-1 confidence in match"
    )
    evidence: Mapped[Optional[dict]] = mapped_column(
        JSONB, comment="Supporting data for the match"
    )
    is_confirmed: Mapped[bool] = mapped_column(
        Boolean, default=False, index=True, comment="Whether match has been manually verified"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_entity_matches_entity1", "entity_type_1", "entity_id_1"),
        Index("ix_entity_matches_entity2", "entity_type_2", "entity_id_2"),
        Index("ix_entity_matches_pair", "entity_type_1", "entity_id_1", "entity_type_2", "entity_id_2"),
    )

    def __repr__(self) -> str:
        return f"<EntityMatch {self.entity_type_1}:{self.entity_id_1} <-> {self.entity_type_2}:{self.entity_id_2}>"


class DebarredEntity(Base):
    """Debarred/excluded vendors from SAM.gov, Comptroller, HHSC, etc."""
    __tablename__ = "debarred_entities"

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(
        String(50), index=True, comment="sam_gov, comptroller, hhsc_oig, etc."
    )
    sam_number: Mapped[Optional[str]] = mapped_column(
        String(50), unique=True, nullable=True, comment="SAM.gov unique identifier"
    )
    entity_name: Mapped[str] = mapped_column(String(500), index=True)
    name_normalized: Mapped[str] = mapped_column(String(500), index=True)
    exclusion_type: Mapped[Optional[str]] = mapped_column(
        String(100), comment="Ineligible Procurement, Reciprocal, etc."
    )
    classification: Mapped[Optional[str]] = mapped_column(
        String(100), comment="Firm, Individual, Special Entity Designation"
    )
    exclusion_program: Mapped[Optional[str]] = mapped_column(
        String(200), comment="Nonprocurement, Reciprocal, Procurement, etc."
    )
    excluding_agency: Mapped[Optional[str]] = mapped_column(
        String(200), comment="Agency that issued the exclusion"
    )
    cage_code: Mapped[Optional[str]] = mapped_column(String(20))
    uei: Mapped[Optional[str]] = mapped_column(
        String(20), comment="Unique Entity Identifier"
    )
    address: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(String(100))
    state: Mapped[Optional[str]] = mapped_column(String(50))
    zip_code: Mapped[Optional[str]] = mapped_column(String(20))
    country: Mapped[Optional[str]] = mapped_column(String(100))
    start_date: Mapped[Optional[date]] = mapped_column(
        Date, index=True, comment="When exclusion became active"
    )
    end_date: Mapped[Optional[date]] = mapped_column(
        Date, index=True, comment="When exclusion terminates (null = indefinite)"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, index=True, comment="Currently excluded"
    )
    reason: Mapped[Optional[str]] = mapped_column(Text, comment="Reason for exclusion")
    raw_data: Mapped[Optional[dict]] = mapped_column(
        JSONB, comment="Full record from source"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("ix_debarred_source_active", "source", "is_active"),
        Index("ix_debarred_dates", "start_date", "end_date"),
    )

    def __repr__(self) -> str:
        return f"<DebarredEntity {self.source}: {self.entity_name}>"
