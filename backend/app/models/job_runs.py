import uuid
from sqlalchemy import Column, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.core.db import Base

class JobRun(Base):
    __tablename__ = "job_runs"

    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name = Column(Text, nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(Text, nullable=False, default="running")
    meta = Column(JSONB, nullable=False, default=dict)
