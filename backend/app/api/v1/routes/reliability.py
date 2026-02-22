from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional

import pytz
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.deps import get_db
from app.api.v1.schemas.reliability import DepartureReliability

router = APIRouter(prefix="/v1", tags=["reliability"])

LONDON = pytz.timezone("Europe/London")


def day_type_for_date(d: date) -> str:
    # Python weekday: Mon=0..Sun=6
    wd = d.weekday()
    if 0 <= wd <= 4:
        return "WEEKDAY"
    if wd == 5:
        return "SATURDAY"
    return "SUNDAY"


def dow_filter_sql(day_type: str) -> str:
    # daily_slot_agg uses Postgres DOW: 0=Sunday..6=Saturday
    if day_type == "WEEKDAY":
        return "day_of_week BETWEEN 1 AND 5"
    if day_type == "SATURDAY":
        return "day_of_week = 6"
    return "day_of_week = 0"


@router.get("/reliability", response_model=list[DepartureReliability])
def get_reliability(
    from_loc: str = Query(..., min_length=3, max_length=3),
    to_loc: str = Query(..., min_length=3, max_length=3),
    date_str: str = Query(..., description="YYYY-MM-DD"),
    arrive_by: str = Query(..., description="HH:MM"),
    operator: Optional[str] = Query(None, description="Optional TOC code e.g. GW"),
    window_minutes: int = Query(120, ge=30, le=360),
    min_services: int = Query(10, ge=1, le=200, description="Min historical services (90d) to include dep_hhmm"),
    db: Session = Depends(get_db),
):
    # Parse date + arrive_by
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    try:
        hh, mm = arrive_by.split(":")
        arrive_t = time(int(hh), int(mm))
    except Exception:
        raise HTTPException(status_code=400, detail="arrive_by must be HH:MM")

    day_type = day_type_for_date(d)
    dow_filter = dow_filter_sql(day_type)

    # Define time window for candidate departures (simple MVP)
    arrive_dt = LONDON.localize(datetime.combine(d, arrive_t))
    window_start = arrive_dt - timedelta(minutes=window_minutes)

    # latest metric date (for daytype table)
    metric_date = db.execute(text("SELECT MAX(metric_date) FROM slot_metrics_daytype")).scalar_one_or_none()
    if metric_date is None:
        raise HTTPException(status_code=500, detail="No slot_metrics_daytype found. Run compute job.")

    # 1) Candidate dep_hhmm list (from daily_slot_agg frequency)
    candidates_sql = text(
        f"""
        SELECT dep_hhmm
        FROM daily_slot_agg
        WHERE origin = :origin
          AND destination = :destination
          AND service_date >= (CURRENT_DATE - INTERVAL '90 days')
          AND ({dow_filter})
          AND ((:operator)::text IS NULL OR operator = (:operator)::text)
        GROUP BY dep_hhmm
        HAVING SUM(n_services) >= :min_services
        ORDER BY dep_hhmm
        """
    )

    dep_rows = db.execute(
        candidates_sql,
        {"origin": from_loc, "destination": to_loc, "operator": operator, "min_services": min_services},
    ).all()

    dep_hhmms = [r[0] for r in dep_rows]

    # Filter to the requested window using the user’s selected date
    filtered_hhmms: list[str] = []
    for hhmm in dep_hhmms:
        dep_time = time(int(hhmm[:2]), int(hhmm[2:]))
        dep_dt = LONDON.localize(datetime.combine(d, dep_time))
        if window_start <= dep_dt <= arrive_dt:
            filtered_hhmms.append(hhmm)

    if not filtered_hhmms:
        return []

    # 2) Fetch slot metrics for those dep_hhmm
    # We fetch all matching rows and build a map dep_hhmm -> metric row.
    metrics_sql = text(
        """
        SELECT
          operator,
          dep_hhmm,
          disruption_prob,
          cancellation_prob,
          reliability_score,
          effective_sample_size,
          confidence_band
        FROM slot_metrics_daytype
        WHERE metric_date = :metric_date
          AND model_version = :model_version
          AND origin = :origin
          AND destination = :destination
          AND day_type = :day_type
          AND ((:operator)::text IS NULL OR operator = (:operator)::text)
          AND dep_hhmm = ANY(:dep_hhmms)
        """
    )

    rows = db.execute(
        metrics_sql,
        {
            "metric_date": metric_date,
            "model_version": "v1_daytype",
            "origin": from_loc,
            "destination": to_loc,
            "day_type": day_type,
            "operator": operator,
            "dep_hhmms": filtered_hhmms,
        },
    ).mappings().all()

    by_hhmm = {r["dep_hhmm"]: r for r in rows}

    # 3) Baseline fallback (operator+day_type+route)
    baseline_sql = text(
        """
        SELECT
          AVG(disruption_prob) AS disruption_prob,
          AVG(cancellation_prob) AS cancellation_prob
        FROM slot_metrics_daytype
        WHERE metric_date = :metric_date
          AND model_version = :model_version
          AND origin = :origin
          AND destination = :destination
          AND day_type = :day_type
          AND ((:operator)::text IS NULL OR operator = (:operator)::text)
        """
    )
    baseline = db.execute(
        baseline_sql,
        {
            "metric_date": metric_date,
            "model_version": "v1_daytype",
            "origin": from_loc,
            "destination": to_loc,
            "day_type": day_type,
            "operator": operator,
        },
    ).mappings().one()

    baseline_disruption = float(baseline["disruption_prob"] or 0.0)
    baseline_cancel = float(baseline["cancellation_prob"] or 0.0)
    baseline_score = int(round(100.0 * (1.0 - baseline_disruption)))

    # 4) Build response (slot metric if exists; else baseline)
    out: list[DepartureReliability] = []
    for hhmm in filtered_hhmms:
        dep_time = time(int(hhmm[:2]), int(hhmm[2:]))
        dep_dt = LONDON.localize(datetime.combine(d, dep_time))

        m = by_hhmm.get(hhmm)
        if m:
            out.append(
                DepartureReliability(
                    departure_time=dep_dt.isoformat(),
                    dep_hhmm=hhmm,
                    operator=m["operator"],
                    disruption_prob=float(m["disruption_prob"]),
                    cancellation_prob=float(m["cancellation_prob"]),
                    reliability_score=int(m["reliability_score"]),
                    effective_sample_size=float(m["effective_sample_size"]),
                    confidence_band=m["confidence_band"],
                    coverage="slot",
                )
            )
        else:
            out.append(
                DepartureReliability(
                    departure_time=dep_dt.isoformat(),
                    dep_hhmm=hhmm,
                    operator=operator,
                    disruption_prob=baseline_disruption,
                    cancellation_prob=baseline_cancel,
                    reliability_score=baseline_score,
                    effective_sample_size=0.0,
                    confidence_band="low",
                    coverage="baseline_fallback",
                )
            )

    return out