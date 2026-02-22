"""
Daily slot rollup job: raw_service_events -> daily_slot_agg

Aggregates per:
  (service_date, operator, origin, destination, dep_hhmm)

Metrics:
  n_services      = total services in that slot
  n_cancelled     = cancelled services
  n_delayed_gt5   = (not cancelled) and arrival_delay_minutes > 5
  n_disrupted     = cancelled OR delayed_gt5

Notes:
- dep_hhmm is derived from scheduled_departure_ts using Postgres to_char(..., 'HH24MI')
- day_of_week uses EXTRACT(DOW FROM service_date) giving 0=Sunday..6=Saturday

Usage:
  run_daily_slot_aggs(db, from_date="2026-01-01", to_date="2026-01-07")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class DailySlotAggResult:
    from_date: str
    to_date: str
    rows_affected: int


_DAILY_SLOT_AGG_SQL = text(
    """
    INSERT INTO daily_slot_agg (
        service_date,
        operator,
        origin,
        destination,
        dep_hhmm,
        day_of_week,
        n_services,
        n_cancelled,
        n_delayed_gt5,
        n_disrupted
    )
    SELECT
        r.service_date                                             AS service_date,
        r.operator                                                 AS operator,
        r.origin                                                   AS origin,
        r.destination                                              AS destination,
        to_char(r.scheduled_departure_ts, 'HH24MI')                AS dep_hhmm,
        EXTRACT(DOW FROM r.service_date)::int                      AS day_of_week,
        COUNT(*)::int                                              AS n_services,
        SUM(CASE WHEN r.cancelled THEN 1 ELSE 0 END)::int          AS n_cancelled,
        SUM(
            CASE
                WHEN NOT r.cancelled
                 AND r.arrival_delay_minutes IS NOT NULL
                 AND r.arrival_delay_minutes > 5
                THEN 1 ELSE 0
            END
        )::int                                                     AS n_delayed_gt5,
        SUM(
            CASE
                WHEN r.cancelled
                 OR (NOT r.cancelled
                     AND r.arrival_delay_minutes IS NOT NULL
                     AND r.arrival_delay_minutes > 5)
                THEN 1 ELSE 0
            END
        )::int                                                     AS n_disrupted
    FROM raw_service_events r
    WHERE r.service_date >= :from_date
      AND r.service_date <= :to_date
      AND ((:operator)::text IS NULL OR r.operator = (:operator)::text)
      AND ((:origin)::text IS NULL OR r.origin = (:origin)::text)
      AND ((:destination)::text IS NULL OR r.destination = (:destination)::text)
    GROUP BY
        r.service_date,
        r.operator,
        r.origin,
        r.destination,
        to_char(r.scheduled_departure_ts, 'HH24MI'),
        EXTRACT(DOW FROM r.service_date)::int
    ON CONFLICT (service_date, operator, origin, destination, dep_hhmm)
    DO UPDATE SET
        day_of_week   = EXCLUDED.day_of_week,
        n_services    = EXCLUDED.n_services,
        n_cancelled   = EXCLUDED.n_cancelled,
        n_delayed_gt5 = EXCLUDED.n_delayed_gt5,
        n_disrupted   = EXCLUDED.n_disrupted
    ;
    """
)


def run_daily_slot_aggs(
    db: Session,
    *,
    from_date: str,
    to_date: str,
    operator: Optional[str] = None,
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    commit: bool = True,
) -> DailySlotAggResult:
    """
    Compute / upsert daily_slot_agg rows for [from_date, to_date] inclusive.

    Args:
      db: SQLAlchemy Session
      from_date/to_date: YYYY-MM-DD strings
      operator/origin/destination: optional filters
      commit: commit transaction if True (default)

    Returns:
      DailySlotAggResult with rows_affected (as reported by driver; may be -1 on some setups)
    """
    res = db.execute(
        _DAILY_SLOT_AGG_SQL,
        {
            "from_date": from_date,
            "to_date": to_date,
            "operator": operator,
            "origin": origin,
            "destination": destination,
        },
    )

    if commit:
        db.commit()

    # rowcount can be -1 depending on driver/execution plan; still useful when available.
    return DailySlotAggResult(from_date=from_date, to_date=to_date, rows_affected=res.rowcount)
