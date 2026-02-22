from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

DAY_MODES_ALL = ("WEEKDAY", "SATURDAY", "SUNDAY")


@dataclass(frozen=True)
class BackfillConfig:
    from_loc: str
    to_loc: str
    from_time: str
    to_time: str
    day_modes: tuple[str, ...]
    toc: list[str]
    chunk_days: int
    lookback_days: int
    # metrics runner args
    model_version: str
    half_life_days: float
    prior_strength: float


def validate_hhmm(value: str) -> str:
    v = value.strip()
    if len(v) != 4 or not v.isdigit():
        raise argparse.ArgumentTypeError(f"Time must be HHMM (e.g. 0630). Got: {value}")
    hh = int(v[:2]); mm = int(v[2:])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise argparse.ArgumentTypeError(f"Invalid HHMM time: {value}")
    return v


def iter_date_chunks(start: date, end: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def run_module(module: str, args: list[str]) -> None:
    """
    Runs: <current python> -m <module> <args...>
    Using sys.executable ensures we run inside the same interpreter (pipenv run python).
    """
    cmd = [sys.executable, "-m", module, *args]
    print("\n$ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def backfill(config: BackfillConfig) -> None:
    today = date.today()
    start = today - timedelta(days=config.lookback_days)
    end = today - timedelta(days=1)

    for day_mode in config.day_modes:
        print("\n==============================")
        print(f" BACKFILL: {day_mode}")
        print(f" Corridor: {config.from_loc} -> {config.to_loc}")
        print(f" Dates:    {start.isoformat()} -> {end.isoformat()} (lookback={config.lookback_days}d)")
        print(f" Times:    {config.from_time} -> {config.to_time}")
        print(f" TOC:      {config.toc or 'None'}")
        print(f" Chunk:    {config.chunk_days} days")
        print("==============================")

        for chunk_start, chunk_end in iter_date_chunks(start, end, config.chunk_days):
            from_date = chunk_start.isoformat()
            to_date = chunk_end.isoformat()

            # 1) INGEST: Darwin HSP -> raw_service_events
            ingest_args = [
                "--source", "hsp",
                "--from-loc", config.from_loc,
                "--to-loc", config.to_loc,
                "--from-date", from_date,
                "--to-date", to_date,
                "--from-time", config.from_time,
                "--to-time", config.to_time,
                "--days", day_mode,
            ]
            for toc in config.toc:
                ingest_args += ["--toc", toc]

            run_module("app.jobs.ingest.run_ingest", ingest_args)

            # 2) ROLLUP: raw_service_events -> daily_slot_agg
            # Your CLI uses --from-loc/--to-loc (dest origin/destination)
            rollup_args = [
                "--from-date", from_date,
                "--to-date", to_date,
                "--from-loc", config.from_loc,
                "--to-loc", config.to_loc,
            ]
            # Filter operator only if a single TOC specified (optional speed-up)
            if len(config.toc) == 1:
                rollup_args += ["--operator", config.toc[0]]

            run_module("app.jobs.rollup.run_daily_slot_agg", rollup_args)

    # 3) METRICS: daily_slot_agg -> slot_metrics (run once at end)
    metrics_args = [
        "--metric-date", today.isoformat(),
        "--from-loc", config.from_loc,
        "--to-loc", config.to_loc,
        "--model-version", config.model_version,
        "--window-days", str(config.lookback_days),
        "--half-life-days", str(config.half_life_days),
        "--prior-strength", str(config.prior_strength),
    ]
    run_module("app.jobs.compute_slot_metrics.run_compute_slot_metrics", metrics_args)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Backfill HSP by running existing scripts as subprocesses (ingest, rollup, metrics)."
    )
    p.add_argument("--from-loc", default="RDG", help="Origin CRS (default: RDG)")
    p.add_argument("--to-loc", default="PAD", help="Destination CRS (default: PAD)")
    p.add_argument("--from-time", default="0000", type=validate_hhmm)
    p.add_argument("--to-time", default="2359", type=validate_hhmm)
    p.add_argument("--lookback-days", type=int, default=90)
    p.add_argument("--chunk-days", type=int, default=7)

    day_group = p.add_mutually_exclusive_group()
    day_group.add_argument("--weekday-only", action="store_true")
    day_group.add_argument("--all-days", action="store_true")
    day_group.add_argument("--day-modes", nargs="+", choices=list(DAY_MODES_ALL))

    p.add_argument("--toc", action="append", default=[], help="Repeatable (e.g. --toc GW). Optional.")

    p.add_argument("--model-version", default="v1")
    p.add_argument("--half-life-days", type=float, default=30.0)
    p.add_argument("--prior-strength", type=float, default=50.0)

    return p


def main():
    args = build_parser().parse_args()

    if args.day_modes:
        day_modes = tuple(args.day_modes)
    elif args.all_days:
        day_modes = ("WEEKDAY", "SATURDAY", "SUNDAY")
    else:
        day_modes = ("WEEKDAY",)

    cfg = BackfillConfig(
        from_loc=args.from_loc,
        to_loc=args.to_loc,
        from_time=args.from_time,
        to_time=args.to_time,
        day_modes=day_modes,
        toc=args.toc or [],
        chunk_days=args.chunk_days,
        lookback_days=args.lookback_days,
        model_version=args.model_version,
        half_life_days=args.half_life_days,
        prior_strength=args.prior_strength,
    )

    print("Starting backfill at:", datetime.now().isoformat(timespec="seconds"))
    backfill(cfg)
    print("Finished backfill at:", datetime.now().isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()