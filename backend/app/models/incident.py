"""Incident-related models — incidents, correlations, RCA, impact, and recommendations."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, Integer, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from app.database import Base
from app.models.base import TimestampMixin


class Incident(TimestampMixin, Base):
    __tablename__ = "incidents"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    severity = Column(String, default="medium")  # low, medium, high, critical
    status = Column(String, default="detected")  # detected, investigating, resolved
    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    affected_services_json = Column(Text, nullable=True)  # JSON list of service names
    event_count = Column(Integer, default=0)

    # Relationships
    dataset = relationship("Dataset", back_populates="incidents")
    incident_events = relationship("IncidentEvent", back_populates="incident", cascade="all, delete-orphan")
    root_causes = relationship("RootCauseCandidate", back_populates="incident", cascade="all, delete-orphan")
    impact_reports = relationship("ImpactReport", back_populates="incident", cascade="all, delete-orphan")
    recommendations = relationship("Recommendation", back_populates="incident", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_incidents_dataset_severity", "dataset_id", "severity"),
    )

    def __repr__(self):
        return f"<Incident {self.title} [{self.severity}]>"


class IncidentEvent(Base):
    __tablename__ = "incident_events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=False, index=True)
    event_id = Column(String, ForeignKey("events.id"), nullable=False, index=True)
    correlation_score = Column(Float, nullable=True)
    correlation_reason = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    incident = relationship("Incident", back_populates="incident_events")
    event = relationship("Event", back_populates="incident_events")

    def __repr__(self):
        return f"<IncidentEvent incident={self.incident_id[:8]} event={self.event_id[:8]}>"


class RootCauseCandidate(Base):
    __tablename__ = "root_cause_candidates"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=False, index=True)
    service_name = Column(String, nullable=False)
    failure_type = Column(String, nullable=False)
    confidence_score = Column(Float, nullable=False)
    evidence_json = Column(Text, nullable=False)  # JSON with supporting records
    explanation = Column(Text, nullable=False)
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    incident = relationship("Incident", back_populates="root_causes")

    def __repr__(self):
        return f"<RootCauseCandidate {self.service_name} ({self.confidence_score:.0%})>"


class ImpactReport(Base):
    __tablename__ = "impact_reports"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=False, index=True)
    affected_service = Column(String, nullable=False)
    impact_type = Column(String, nullable=False)  # direct, downstream, upstream
    severity = Column(String, nullable=False)
    affected_event_count = Column(Integer, default=0)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    incident = relationship("Incident", back_populates="impact_reports")

    def __repr__(self):
        return f"<ImpactReport {self.affected_service} ({self.impact_type})>"


class Recommendation(Base):
    __tablename__ = "recommendations"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    incident_id = Column(String, ForeignKey("incidents.id"), nullable=False, index=True)
    priority = Column(Integer, default=1)
    category = Column(String, nullable=False)  # investigation, mitigation, prevention
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    evidence_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    incident = relationship("Incident", back_populates="recommendations")

    def __repr__(self):
        return f"<Recommendation #{self.priority} {self.title}>"
