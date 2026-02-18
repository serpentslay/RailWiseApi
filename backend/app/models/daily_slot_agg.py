from sqlalchemy import Column, Date, Integer, Text
from app.core.db import Base

class DailySlotAgg(Base):
    __tablename__ = "daily_slot_agg"

    service_date = Column(Date, primary_key=True)
    operator = Column(Text, primary_key=True)
    origin = Column(Text, primary_key=True)
    destination = Column(Text, primary_key=True)
    dep_hhmm = Column(Text, primary_key=True)

    day_of_week = Column(Integer, nullable=False, index=True)

    n_services = Column(Integer, nullable=False)
    n_cancelled = Column(Integer, nullable=False)
    n_delayed_gt5 = Column(Integer, nullable=False)
    n_disrupted = Column(Integer, nullable=False)
