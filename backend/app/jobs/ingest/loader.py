import uuid
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.models.raw_service_events import RawServiceEvent
from app.jobs.ingest.types import CanonicalServiceEvent

def load_events(db: Session, events: list[CanonicalServiceEvent], source_run_id: uuid.UUID) -> dict:
    """
    Insert canonical events into raw_service_events idempotently.
    Requires a UNIQUE constraint on:
      (service_date, operator, origin, destination, scheduled_departure_ts)
    """
    inserted = 0
    skipped = 0

    for i, ev in enumerate(events, start=1):
        stmt = (
            insert(RawServiceEvent)
            .values(
                service_date=ev.service_date,
                operator=ev.operator,
                origin=ev.origin,
                destination=ev.destination,
                scheduled_departure_ts=ev.scheduled_departure_ts,
                scheduled_arrival_ts=ev.scheduled_arrival_ts,
                actual_arrival_ts=ev.actual_arrival_ts,
                cancelled=ev.cancelled,
                arrival_delay_minutes=ev.arrival_delay_minutes,
                service_key=ev.service_key,
                source_run_id=source_run_id,
                sourced=ev.source
            )
            .on_conflict_do_nothing(
                index_elements=[
                    "service_key"
                ]
            )
            .returning(RawServiceEvent.id)
        )

        res = db.execute(stmt)
        row = res.fetchone()
        if row == 1:
            inserted += 1
        else:
            skipped += 1

        # batch commits to keep memory small and speed stable
        if i % 1000 == 0:
            db.commit()

    db.commit()
    return {"total": len(events), "inserted": inserted, "skipped": skipped}
