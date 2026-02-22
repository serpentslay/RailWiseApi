import argparse
from datetime import date

from app.core.db import SessionLocal
from app.jobs.compute_slot_metrics.compute_slot_metrics import compute_slot_metrics

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--metric-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--operator")
    p.add_argument("--model-version", default="v1")
    p.add_argument("--window-days", type=int, default=90)
    p.add_argument("--half-life-days", type=float, default=30.0)
    p.add_argument("--prior-strength", type=float, default=50.0)
    args = p.parse_args()

    metric_date = date.fromisoformat(args.metric_date)

    db = SessionLocal()
    try:
        res = compute_slot_metrics(
            db,
            metric_date=metric_date,
            model_version=args.model_version,
            window_days=args.window_days,
            half_life_days=args.half_life_days,
            prior_strength=args.prior_strength,
            operator=args.operator,
            origin=args.origin,
            destination=args.destination,
            commit=True,
        )
        print(res)
    finally:
        db.close()

if __name__ == "__main__":
    main()