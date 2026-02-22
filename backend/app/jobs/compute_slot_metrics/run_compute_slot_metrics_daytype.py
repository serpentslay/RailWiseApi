import argparse
from datetime import date

from app.core.db import SessionLocal
from app.jobs.compute_slot_metrics.compute_slot_metrics_daytype import compute_slot_metrics_daytype


def main():
    p = argparse.ArgumentParser(description="Compute slot_metrics_daytype from daily_slot_agg")
    p.add_argument("--metric-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--model-version", default="v1_daytype")
    p.add_argument("--window-days", type=int, default=90)
    p.add_argument("--half-life-days", type=float, default=30.0)
    p.add_argument("--prior-strength", type=float, default=10.0)

    p.add_argument("--operator")
    p.add_argument("--from-loc", dest="origin")
    p.add_argument("--to-loc", dest="destination")

    args = p.parse_args()

    db = SessionLocal()
    try:
        res = compute_slot_metrics_daytype(
            db,
            metric_date=date.fromisoformat(args.metric_date),
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