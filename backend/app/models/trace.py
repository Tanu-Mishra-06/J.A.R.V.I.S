"""Trace model — a single span within a distributed trace."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from app.database import Base


class Trace(Base):
    __tablename__ = "traces"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False, index=True)
    trace_id = Column(String, index=True, nullable=False)
    span_id = Column(String, nullable=False)
    parent_span_id = Column(String, nullable=True)
    service_name = Column(String, index=True, nullable=True)
    operation_name = Column(String, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_ms = Column(Float, nullable=True)
    status_code = Column(String, nullable=True)  # "ok", "error"
    attributes_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    dataset = relationship("Dataset", back_populates="traces")

    __table_args__ = (
        Index("ix_traces_dataset_trace", "dataset_id", "trace_id"),
    )

    def __repr__(self):
        return f"<Trace {self.trace_id[:8]}/{self.span_id[:8]} ({self.service_name})>"
