"""Dataset model — represents an uploaded or demo data source."""

import uuid

from sqlalchemy import Column, String, Integer, Boolean, Text
from sqlalchemy.orm import relationship

from app.database import Base
from app.models.base import TimestampMixin


class Dataset(TimestampMixin, Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    source_type = Column(String, nullable=True)  # csv, json, jsonl, log
    file_path = Column(String, nullable=True)
    record_count = Column(Integer, default=0)
    valid_records = Column(Integer, default=0)
    invalid_records = Column(Integer, default=0)
    status = Column(String, default="uploaded")  # uploaded, processing, processed, error
    error_message = Column(String, nullable=True)
    metadata_json = Column(Text, nullable=True)
    is_demo = Column(Boolean, default=False)

    # Relationships
    events = relationship("Event", back_populates="dataset", cascade="all, delete-orphan")
    metrics = relationship("Metric", back_populates="dataset", cascade="all, delete-orphan")
    traces = relationship("Trace", back_populates="dataset", cascade="all, delete-orphan")
    incidents = relationship("Incident", back_populates="dataset", cascade="all, delete-orphan")
    analysis_jobs = relationship("AnalysisJob", back_populates="dataset", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Dataset {self.name} ({self.status})>"
