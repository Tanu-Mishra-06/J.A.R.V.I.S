"""Metric model — a single time-series metric data point."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from app.database import Base


class Metric(Base):
    __tablename__ = "metrics"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False, index=True)
    timestamp = Column(DateTime, index=True, nullable=True)
    service_name = Column(String, index=True, nullable=True)
    host_name = Column(String, nullable=True)
    metric_name = Column(String, nullable=False)  # cpu_usage, memory_usage, latency_ms, etc.
    metric_value = Column(Float, nullable=False)
    unit = Column(String, nullable=True)
    attributes_json = Column(Text, nullable=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_score = Column(Float, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    dataset = relationship("Dataset", back_populates="metrics")

    __table_args__ = (
        Index("ix_metrics_dataset_timestamp", "dataset_id", "timestamp"),
        Index("ix_metrics_service_metric", "service_name", "metric_name"),
    )

    def __repr__(self):
        return f"<Metric {self.metric_name}={self.metric_value} ({self.service_name})>"
