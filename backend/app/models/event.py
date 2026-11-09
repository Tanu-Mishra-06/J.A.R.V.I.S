"""Event model — a single observability event (log entry, alert, etc.)."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from app.database import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False, index=True)
    timestamp = Column(DateTime, index=True, nullable=True)
    source = Column(String, nullable=True)  # e.g. "LogHub", "synthetic"
    service_name = Column(String, index=True, nullable=True)
    host_name = Column(String, nullable=True)
    event_type = Column(String, nullable=True)  # "error", "warning", "connection_timeout", etc.
    severity = Column(String, default="info")  # debug, info, warning, error, critical
    message = Column(Text, nullable=True)
    trace_id = Column(String, nullable=True)
    span_id = Column(String, nullable=True)
    parent_span_id = Column(String, nullable=True)
    attributes_json = Column(Text, nullable=True)
    raw_record_json = Column(Text, nullable=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_score = Column(Float, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    dataset = relationship("Dataset", back_populates="events")
    incident_events = relationship("IncidentEvent", back_populates="event", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_events_dataset_timestamp", "dataset_id", "timestamp"),
        Index("ix_events_dataset_severity", "dataset_id", "severity"),
    )

    def __repr__(self):
        return f"<Event {self.id[:8]} [{self.severity}] {self.service_name}>"
