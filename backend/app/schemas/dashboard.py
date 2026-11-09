"""Pydantic schemas for Dashboard API responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SeverityDistribution(BaseModel):
    debug: int = 0
    info: int = 0
    warning: int = 0
    error: int = 0
    critical: int = 0


class ServiceHealthResponse(BaseModel):
    service_name: str
    total_events: int = 0
    error_count: int = 0
    warning_count: int = 0
    anomaly_count: int = 0
    error_rate: float = 0.0
    health_score: float = 100.0  # 0–100, 100 = fully healthy


class TimeSeriesPoint(BaseModel):
    timestamp: str  # ISO string bucketed by hour/minute
    value: float


class DashboardSummary(BaseModel):
    total_events: int = 0
    total_metrics: int = 0
    total_traces: int = 0
    total_incidents: int = 0
    total_services: int = 0
    total_anomalies: int = 0
    total_datasets: int = 0
    severity_distribution: SeverityDistribution = SeverityDistribution()
    services: list[ServiceHealthResponse] = []


class DashboardMetricsResponse(BaseModel):
    event_volume: list[TimeSeriesPoint] = []
    anomaly_rate: list[TimeSeriesPoint] = []


class RecentIncidentItem(BaseModel):
    id: str
    title: str
    severity: str
    status: str
    started_at: Optional[datetime] = None
    event_count: int = 0
    affected_services_json: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}
