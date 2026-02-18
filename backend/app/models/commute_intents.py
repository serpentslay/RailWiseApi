import uuid
from sqlalchemy import Column, DateTime, Float, Text, Boolean
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.core.db import Base

class CommuteIntent(Base):
    __tablename__ = "commute_intents"

    intent_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    search_params = Column(JSONB, nullable=False, default=dict)

    baseline_service_key = Column(Text, nullable=True)
    alt_service_key = Column(Text, nullable=True)

    recommendation_shown = Column(Boolean, nullable=False, default=False)
    risk_delta = Column(Float, nullable=True)

    final_service_key = Column(Text, nullable=True)
    decision = Column(Text, nullable=True)

    outcome = Column(Text, nullable=True)
    outcome_reported_at = Column(DateTime(timezone=True), nullable=True)
