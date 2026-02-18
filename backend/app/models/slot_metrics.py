from sqlalchemy import Column, Date, DateTime, Float, Integer, Text
from sqlalchemy.sql import func
from app.core.db import Base

class SlotMetric(Base):
    __tablename__ = "slot_metrics"

    metric_date = Column(Date, primary_key=True)
    model_version = Column(Text, primary_key=True)

    operator = Column(Text, primary_key=True)
    origin = Column(Text, primary_key=True)
    destination = Column(Text, primary_key=True)
    day_of_week = Column(Integer, primary_key=True)
    dep_hhmm = Column(Text, primary_key=True)

    disruption_prob = Column(Float, nullable=False)
    cancellation_prob = Column(Float, nullable=False)
    reliability_score = Column(Integer, nullable=False)

    effective_sample_size = Column(Float, nullable=False)
    confidence_band = Column(Text, nullable=False)

    computed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
