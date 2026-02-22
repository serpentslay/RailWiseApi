from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class CanonicalServiceEvent:
    # In the future, you can persist these in DB if you add columns
    source: str                      # "hsp"
    source_event_id: Optional[str]   # HSP RID (optional for now)

    service_date: str                # "YYYY-MM-DD"
    operator: str                    # toc_code
    origin: str                      # CRS
    destination: str                 # CRS

    scheduled_departure_ts: datetime
    scheduled_arrival_ts: Optional[datetime]

    actual_arrival_ts: Optional[datetime]
    cancelled: bool
    arrival_delay_minutes: Optional[int]

    service_key: str                 # stable internal join key
