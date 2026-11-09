"""AnalysisJob model — tracks background analysis pipeline runs."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from app.database import Base


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False, index=True)
    job_type = Column(String, nullable=False)  # full, anomaly_only, correlation_only
    status = Column(String, default="pending")  # pending, running, completed, failed
    progress = Column(Float, default=0.0)  # 0–100
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    result_summary_json = Column(Text, nullable=True)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    dataset = relationship("Dataset", back_populates="analysis_jobs")

    def __repr__(self):
        return f"<AnalysisJob {self.job_type} [{self.status}] {self.progress:.0f}%>"
