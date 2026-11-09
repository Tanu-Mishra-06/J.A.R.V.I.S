"""Pydantic schemas for Incident API responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class RootCauseResponse(BaseModel):
    id: str
    incident_id: str
    service_name: str
    failure_type: str
    confidence_score: float
    evidence_json: Optional[str] = None
    explanation: str
    is_primary: bool = False
    created_at: datetime

    model_config = {"from_attributes": True}


class ImpactReportResponse(BaseModel):
    id: str
    incident_id: str
    affected_service: str
    impact_type: str
    severity: str
    affected_event_count: int = 0
    description: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class RecommendationResponse(BaseModel):
    id: str
    incident_id: str
    priority: int
    category: str
    title: str
    description: str
    evidence_json: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class TimelineEventResponse(BaseModel):
    """A single entry in an incident's chronological timeline."""
    timestamp: Optional[datetime] = None
    event_id: str
    service_name: Optional[str] = None
    severity: str = "info"
    event_type: Optional[str] = None
    message: Optional[str] = None
    is_anomaly: bool = False
    correlation_score: Optional[float] = None
    correlation_reason: Optional[str] = None


class IncidentResponse(BaseModel):
    id: str
    dataset_id: str
    title: str
    description: Optional[str] = None
    severity: str = "medium"
    status: str = "detected"
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    affected_services_json: Optional[str] = None
    event_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class IncidentDetailResponse(BaseModel):
    id: str
    dataset_id: str
    title: str
    description: Optional[str] = None
    severity: str = "medium"
    status: str = "detected"
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    affected_services_json: Optional[str] = None
    event_count: int = 0
    created_at: datetime
    updated_at: datetime
    root_causes: list[RootCauseResponse] = []
    impact_reports: list[ImpactReportResponse] = []
    recommendations: list[RecommendationResponse] = []

    model_config = {"from_attributes": True}


class IncidentListResponse(BaseModel):
    incidents: list[IncidentResponse]
    total: int
    page: int
    page_size: int
