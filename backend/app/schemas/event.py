"""Pydantic schemas for Event API requests and responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class EventResponse(BaseModel):
    id: str
    dataset_id: str
    timestamp: Optional[datetime] = None
    source: Optional[str] = None
    service_name: Optional[str] = None
    host_name: Optional[str] = None
    event_type: Optional[str] = None
    severity: str = "info"
    message: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    attributes_json: Optional[str] = None
    raw_record_json: Optional[str] = None
    is_anomaly: bool = False
    anomaly_score: Optional[float] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class EventListResponse(BaseModel):
    events: list[EventResponse]
    total: int
    page: int
    page_size: int


class EventFilterParams(BaseModel):
    """Query parameters for filtering events."""
    dataset_id: Optional[str] = None
    service_name: Optional[str] = None
    severity: Optional[str] = None
    event_type: Optional[str] = None
    is_anomaly: Optional[bool] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    search: Optional[str] = None
    page: int = 1
    page_size: int = 50
