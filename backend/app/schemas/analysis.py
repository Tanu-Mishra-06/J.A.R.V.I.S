"""Pydantic schemas for Analysis Job API requests and responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AnalysisJobCreate(BaseModel):
    dataset_id: str
    job_type: str = "full"  # full, anomaly_only, correlation_only


class AnalysisJobResponse(BaseModel):
    id: str
    dataset_id: str
    job_type: str
    status: str
    progress: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result_summary_json: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}
