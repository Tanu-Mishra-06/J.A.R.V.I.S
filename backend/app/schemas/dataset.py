"""Pydantic schemas for Dataset API requests and responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class DatasetUploadResponse(BaseModel):
    id: str
    name: str
    source_type: Optional[str] = None
    file_path: Optional[str] = None
    status: str
    is_demo: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class DatasetListItem(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    source_type: Optional[str] = None
    record_count: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    status: str
    is_demo: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class DatasetListResponse(BaseModel):
    datasets: list[DatasetListItem]
    total: int
    page: int
    page_size: int


class DatasetDetailResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    source_type: Optional[str] = None
    file_path: Optional[str] = None
    record_count: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    status: str
    error_message: Optional[str] = None
    metadata_json: Optional[str] = None
    is_demo: bool
    created_at: datetime
    updated_at: datetime
    event_count: int = 0
    metric_count: int = 0
    trace_count: int = 0

    model_config = {"from_attributes": True}
