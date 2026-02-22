import argparse
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.models.job_runs import JobRun
from app.jobs.rollup.daily_slot_agg import run_daily_slot_aggs


def _count_daily_slot_aggs(
    db: Session,
    *,
    from_date: str,
    to_date: str,
    operator: Optional[str],
    origin: Optional[str],
    destination: Optional[str],
) -> int:
    """
    Rowcount for INSERT..ON CONFLICT is unreliable across drivers, so we do a
    before/after COUNT(*) for the range/filters.
    """
    q = text(
        """
        SELECT COUNT(*)
        FROM daily_slot_agg
        WHERE service_date >= :from_date
          AND service_date <= :to_date
          AND ((:operator)::text IS NULL OR operator = (:operator)::text)
          AND ((:origin)::text IS NULL OR origin = (:origin)::text)
          AND ((:destination)::text IS NULL OR destination = (:destination)::text)
        """
    )
    return int(
        db.execute(
            q,
            {
                "from_date": from_date,
                "to_date": to_date,
                "operator": operator,
                "origin": origin,
                "destination": destination,
            },
        ).scalar_one()
    )


def main():
    p = argparse.ArgumentParser(description="Roll up raw_service_events into daily_slot_agg")

    p.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--to-date", required=True, help="YYYY-MM-DD")

    # Optional filters (mirror ingestion-ish args but without times/days)
    p.add_argument("--operator", help="Optional operator filter (e.g. GW)")
    p.add_argument("--from-loc", dest="origin", help="Optional origin CRS filter (e.g. RDG)")
    p.add_argument("--to-loc", dest="destination", help="Optional destination CRS filter (e.g. PAD)")

    args = p.parse_args()

    db: Session = SessionLocal()
    run_id = uuid.uuid4()

    job = JobRun(
        run_id=run_id,
        job_name="rollup_daily_slot_agg",
        status="running",
        meta={"args": vars(args)},
    )
    db.add(job)
    db.commit()

    try:
        before = _count_daily_slot_aggs(
            db,
            from_date=args.from_date,
            to_date=args.to_date,
            operator=args.operator,
            origin=args.origin,
            destination=args.destination,
        )

        result = run_daily_slot_aggs(
            db,
            from_date=args.from_date,
            to_date=args.to_date,
            operator=args.operator,
            origin=args.origin,
            destination=args.destination,
            commit=True,
        )

        after = _count_daily_slot_aggs(
            db,
            from_date=args.from_date,
            to_date=args.to_date,
            operator=args.operator,
            origin=args.origin,
            destination=args.destination,
        )

        result_payload = {
            "from_date": args.from_date,
            "to_date": args.to_date,
            "operator": args.operator,
            "origin": args.origin,
            "destination": args.destination,
            # counts that work regardless of rowcount support
            "rows_before": before,
            "rows_after": after,
            "rows_net_new": max(0, after - before),
        }

        job = db.get(JobRun, run_id)
        job.status = "success"
        job.ended_at = datetime.utcnow()
        job.meta = {**(job.meta or {}), **result_payload}
        db.commit()

        print(result_payload)

    except Exception as e:
        db.rollback()
        job = db.get(JobRun, run_id)
        job.status = "fail"
        job.ended_at = datetime.utcnow()
        job.meta = {**(job.meta or {}), "error": repr(e)}
        db.commit()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    main()
