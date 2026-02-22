from sqlalchemy import Column, Date, Float, Integer, Text, DateTime, func
from app.core.db import Base

class SlotMetricsDayType(Base):
    __tablename__ = "slot_metrics_daytype"

    metric_date = Column(Date, primary_key=True)
    model_version = Column(Text, primary_key=True)

    operator = Column(Text, primary_key=True)
    origin = Column(Text, primary_key=True)
    destination = Column(Text, primary_key=True)

    day_type = Column(Text, primary_key=True)   # WEEKDAY / SATURDAY / SUNDAY
    dep_hhmm = Column(Text, primary_key=True)

    disruption_prob = Column(Float, nullable=False)
    cancellation_prob = Column(Float, nullable=False)
    reliability_score = Column(Integer, nullable=False)

    effective_sample_size = Column(Float, nullable=False)
    confidence_band = Column(Text, nullable=False)

    computed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)