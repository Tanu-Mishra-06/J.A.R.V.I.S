"""Service and ServiceDependency models — discovered service topology."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from app.database import Base


class Service(Base):
    __tablename__ = "services"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, nullable=False)
    service_type = Column(String, nullable=True)  # web, database, cache, queue, gateway
    description = Column(String, nullable=True)
    metadata_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships — outgoing and incoming edges in the dependency graph
    outgoing_deps = relationship(
        "ServiceDependency",
        foreign_keys="ServiceDependency.source_service_id",
        back_populates="source_service",
        cascade="all, delete-orphan",
    )
    incoming_deps = relationship(
        "ServiceDependency",
        foreign_keys="ServiceDependency.target_service_id",
        back_populates="target_service",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<Service {self.name}>"


class ServiceDependency(Base):
    __tablename__ = "service_dependencies"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_service_id = Column(String, ForeignKey("services.id"), nullable=False)
    target_service_id = Column(String, ForeignKey("services.id"), nullable=False)
    dependency_type = Column(String, default="calls")  # calls, reads, writes, publishes, subscribes
    weight = Column(Float, default=1.0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    source_service = relationship("Service", foreign_keys=[source_service_id], back_populates="outgoing_deps")
    target_service = relationship("Service", foreign_keys=[target_service_id], back_populates="incoming_deps")

    def __repr__(self):
        return f"<ServiceDependency {self.source_service_id[:8]} -> {self.target_service_id[:8]}>"
