from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.job_runs import JobRun
from app.scoring.v1.slot_metrics import (
    accumulate_weighted_counts,
    compute_slot_metric,
)

DAY_TYPES = ("WEEKDAY", "SATURDAY", "SUNDAY")


def dow_to_day_type(dow: int) -> str:
    # Postgres EXTRACT(DOW): 0=Sunday ... 6=Saturday
    if 1 <= dow <= 5:
        return "WEEKDAY"
    if dow == 6:
        return "SATURDAY"
    return "SUNDAY"


@dataclass(frozen=True)
class ComputeSlotMetricsDayTypeResult:
    metric_date: str
    model_version: str
    window_days: int
    half_life_days: float
    prior_strength: float
    slots_written: int
    operators_seen: int


_SELECT_DAILY_ROWS = text(
    """
    SELECT
      service_date,
      operator,
      origin,
      destination,
      dep_hhmm,
      day_of_week,
      n_services,
      n_cancelled,
      n_disrupted
    FROM daily_slot_agg
    WHERE service_date >= :from_date
      AND service_date <= :to_date
      AND ((:operator)::text IS NULL OR operator = (:operator)::text)
      AND ((:origin)::text IS NULL OR origin = (:origin)::text)
      AND ((:destination)::text IS NULL OR destination = (:destination)::text)
    ORDER BY service_date ASC
    """
)

_UPSERT_METRICS = text(
    """
    INSERT INTO slot_metrics_daytype (
      metric_date,
      model_version,
      operator,
      origin,
      destination,
      day_type,
      dep_hhmm,
      disruption_prob,
      cancellation_prob,
      reliability_score,
      effective_sample_size,
      confidence_band
    )
    VALUES (
      :metric_date,
      :model_version,
      :operator,
      :origin,
      :destination,
      :day_type,
      :dep_hhmm,
      :disruption_prob,
      :cancellation_prob,
      :reliability_score,
      :effective_sample_size,
      :confidence_band
    )
    ON CONFLICT (metric_date, model_version, operator, origin, destination, day_type, dep_hhmm)
    DO UPDATE SET
      disruption_prob = EXCLUDED.disruption_prob,
      cancellation_prob = EXCLUDED.cancellation_prob,
      reliability_score = EXCLUDED.reliability_score,
      effective_sample_size = EXCLUDED.effective_sample_size,
      confidence_band = EXCLUDED.confidence_band
    ;
    """
)


def _start_job(db: Session, meta: dict) -> uuid.UUID:
    run_id = uuid.uuid4()
    jr = JobRun(run_id=run_id, job_name="compute_slot_metrics_daytype", status="running", meta=meta)
    db.add(jr)
    db.commit()
    return run_id


def _finish_job(db: Session, run_id: uuid.UUID, status: str, meta_updates: dict) -> None:
    jr = db.get(JobRun, run_id)
    jr.status = status
    jr.ended_at = datetime.utcnow()
    jr.meta = {**(jr.meta or {}), **meta_updates}
    db.commit()


def compute_slot_metrics_daytype(
    db: Session,
    *,
    metric_date: date,
    model_version: str = "v1_daytype",
    window_days: int = 90,
    half_life_days: float = 30.0,
    prior_strength: float = 10.0,
    operator: Optional[str] = None,
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    commit: bool = True,
) -> ComputeSlotMetricsDayTypeResult:
    """
    Computes slot-level metrics grouped by DAY_TYPE (WEEKDAY/SATURDAY/SUNDAY)
    rather than day_of_week.

    Window: [metric_date - window_days, metric_date - 1]
    """
    from_date = metric_date - timedelta(days=window_days)
    to_date = metric_date - timedelta(days=1)

    run_id = _start_job(
        db,
        {
            "metric_date": metric_date.isoformat(),
            "model_version": model_version,
            "window_days": window_days,
            "half_life_days": half_life_days,
            "prior_strength": prior_strength,
            "filters": {"operator": operator, "origin": origin, "destination": destination},
        },
    )

    try:
        rows = db.execute(
            _SELECT_DAILY_ROWS,
            {
                "from_date": from_date.isoformat(),
                "to_date": to_date.isoformat(),
                "operator": operator,
                "origin": origin,
                "destination": destination,
            },
        ).mappings().all()

        if not rows:
            result = ComputeSlotMetricsDayTypeResult(
                metric_date=metric_date.isoformat(),
                model_version=model_version,
                window_days=window_days,
                half_life_days=half_life_days,
                prior_strength=prior_strength,
                slots_written=0,
                operators_seen=0,
            )
            _finish_job(db, run_id, "success", {"result": result.__dict__, "note": "no rows in window"})
            return result

        # Add derived day_type to each row (still dict-like)
        enriched = []
        for r in rows:
            r = dict(r)
            r["day_type"] = dow_to_day_type(int(r["day_of_week"]))
            enriched.append(r)

        # Operator priors: weighted baseline across all day_types/dep_hhmm for that operator
        operator_to_rows: dict[str, list[dict]] = {}
        for r in enriched:
            operator_to_rows.setdefault(r["operator"], []).append(r)

        operator_prior_disruption: dict[str, float] = {}
        operator_prior_cancel: dict[str, float] = {}

        for op, op_rows in operator_to_rows.items():
            w_counts = accumulate_weighted_counts(
                metric_date=metric_date,
                rows=op_rows,
                half_life_days=half_life_days,
            )
            if w_counts.w_services > 0:
                operator_prior_disruption[op] = float(w_counts.w_disrupted / w_counts.w_services)
                operator_prior_cancel[op] = float(w_counts.w_cancelled / w_counts.w_services)
            else:
                operator_prior_disruption[op] = 0.0
                operator_prior_cancel[op] = 0.0

        # Group by DAY_TYPE instead of DOW
        # Key: (operator, origin, destination, day_type, dep_hhmm)
        slot_to_rows: dict[tuple, list[dict]] = {}
        for r in enriched:
            key = (r["operator"], r["origin"], r["destination"], r["day_type"], r["dep_hhmm"])
            slot_to_rows.setdefault(key, []).append(r)

        slots_written = 0
        for (op, org, dst, day_type, hhmm), slot_rows in slot_to_rows.items():
            w_counts = accumulate_weighted_counts(
                metric_date=metric_date,
                rows=slot_rows,
                half_life_days=half_life_days,
            )

            computed = compute_slot_metric(
                w_counts=w_counts,
                operator_prior_disruption=operator_prior_disruption.get(op, 0.0),
                operator_prior_cancel=operator_prior_cancel.get(op, 0.0),
                prior_strength=prior_strength,
            )

            db.execute(
                _UPSERT_METRICS,
                {
                    "metric_date": metric_date.isoformat(),
                    "model_version": model_version,
                    "operator": op,
                    "origin": org,
                    "destination": dst,
                    "day_type": day_type,
                    "dep_hhmm": hhmm,
                    "disruption_prob": float(computed.disruption_prob),
                    "cancellation_prob": float(computed.cancellation_prob),
                    "reliability_score": int(computed.reliability_score),
                    "effective_sample_size": float(computed.effective_sample_size),
                    "confidence_band": computed.confidence_band,
                },
            )
            slots_written += 1

            if commit and slots_written % 500 == 0:
                db.commit()

        if commit:
            db.commit()

        result = ComputeSlotMetricsDayTypeResult(
            metric_date=metric_date.isoformat(),
            model_version=model_version,
            window_days=window_days,
            half_life_days=half_life_days,
            prior_strength=prior_strength,
            slots_written=slots_written,
            operators_seen=len(operator_to_rows),
        )
        _finish_job(db, run_id, "success", {"result": result.__dict__})
        return result

    except Exception as e:
        db.rollback()
        _finish_job(db, run_id, "fail", {"error": repr(e)})
        raise