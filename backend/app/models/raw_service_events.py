import uuid
from sqlalchemy import Column, Date, DateTime, Integer, Boolean, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.core.db import Base

class RawServiceEvent(Base):
    __tablename__ = "raw_service_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    service_date = Column(Date, nullable=False, index=True)
    operator = Column(Text, nullable=False, index=True)
    origin = Column(Text, nullable=False, index=True)
    destination = Column(Text, nullable=False, index=True)

    scheduled_departure_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    scheduled_arrival_ts = Column(DateTime(timezone=True), nullable=True)
    actual_arrival_ts = Column(DateTime(timezone=True), nullable=True)

    cancelled = Column(Boolean, nullable=False, default=False)
    arrival_delay_minutes = Column(Integer, nullable=True)

    service_key = Column(Text, nullable=False, index=True, unique=True)

    source_run_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    source_event_id = Column(Text, nullable=True, index=True)
    sourced = Column(Text, nullable=False, index=True)
