import argparse
import uuid
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.models.job_runs import JobRun
from app.jobs.ingest.registry import SOURCES

def main():
    p = argparse.ArgumentParser(description="Ingest rail performance data into raw_service_events")
    p.add_argument("--source", required=True, choices=SOURCES.keys())

    p.add_argument("--from-loc", required=True, help="CRS code (e.g. RDG)")
    p.add_argument("--to-loc", required=True, help="CRS code (e.g. PAD)")

    p.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--to-date", required=True, help="YYYY-MM-DD")

    p.add_argument("--from-time", required=True, help="HHMM (e.g. 0630)")
    p.add_argument("--to-time", required=True, help="HHMM (e.g. 0930)")

    p.add_argument("--days", required=True, choices=["WEEKDAY", "SATURDAY", "SUNDAY"])
    p.add_argument("--toc", action="append", help="Optional TOC code filter; repeatable (e.g. --toc GW)")

    args = p.parse_args()

    db: Session = SessionLocal()
    run_id = uuid.uuid4()

    job = JobRun(
        run_id=run_id,
        job_name=f"ingest_{args.source}",
        status="running",
        meta={"args": vars(args)},
    )
    db.add(job)
    db.commit()

    try:
        source = SOURCES[args.source]()  # instantiate adapter
        result = source.ingest(
            db=db,
            run_id=run_id,
            from_loc=args.from_loc,
            to_loc=args.to_loc,
            from_date=args.from_date,
            to_date=args.to_date,
            from_time=args.from_time,
            to_time=args.to_time,
            days=args.days,
            toc_filter=args.toc,
        )

        job = db.get(JobRun, run_id)
        job.status = "success"
        job.ended_at = datetime.utcnow()
        job.meta = {**(job.meta or {}), **result}
        db.commit()

        print(result)

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
