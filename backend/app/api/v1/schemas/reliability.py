from pydantic import BaseModel, Field
from typing import Literal, Optional

class DepartureReliability(BaseModel):
    departure_time: str = Field(..., description="ISO datetime in Europe/London")
    dep_hhmm: str

    operator: Optional[str] = None

    disruption_prob: float
    cancellation_prob: float
    reliability_score: int
    effective_sample_size: float
    confidence_band: Literal["low", "medium", "high"]

    coverage: Literal["slot", "baseline_fallback"]