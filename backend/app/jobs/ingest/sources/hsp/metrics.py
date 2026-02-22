import logging
from datetime import date as Date, datetime, timedelta
from typing import Optional

import httpx

from .config import HspConfig
from .http import post_with_retry

logger = logging.getLogger(__name__)


def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def parse_hhmm(hhmm: str) -> tuple[int, int]:
    hhmm = hhmm.strip()
    return int(hhmm[:2]), int(hhmm[2:])


def fmt_hhmm(h: int, m: int) -> str:
    return f"{h:02d}{m:02d}"


def time_windows(from_time: str, to_time: str, step_minutes: int) -> list[tuple[str, str]]:
    """Produce HHMM windows [start, end], stepping by step_minutes. Assumes same-day window."""
    sh, sm = parse_hhmm(from_time)
    eh, em = parse_hhmm(to_time)

    start = datetime(2000, 1, 1, sh, sm)
    end = datetime(2000, 1, 1, eh, em)

    windows: list[tuple[str, str]] = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(minutes=step_minutes), end)
        windows.append((fmt_hhmm(cur.hour, cur.minute), fmt_hhmm(nxt.hour, nxt.minute)))
        cur = nxt
    return windows


def date_range(from_date: str, to_date: str) -> list[str]:
    d0 = Date.fromisoformat(from_date)
    d1 = Date.fromisoformat(to_date)
    out: list[str] = []
    d = d0
    while d <= d1:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def weekday_only(dates: list[str]) -> list[str]:
    """Filter ISO date strings to Mon–Fri."""
    return [s for s in dates if Date.fromisoformat(s).weekday() < 5]


def fetch_service_metrics_chunked(
    cfg: HspConfig,
    client: httpx.Client,
    *,
    from_loc: str,
    to_loc: str,
    from_date: str,
    to_date: str,
    from_time: str,
    to_time: str,
    days: str,
    toc_filter: Optional[list[str]],
) -> list[dict]:
    """Calls /serviceMetrics in smaller chunks; returns merged list of Services entries."""
    base_payload = {"from_loc": from_loc, "to_loc": to_loc, "days": days}
    if toc_filter:
        base_payload["toc_filter"] = toc_filter

    dates = date_range(from_date, to_date)
    if cfg.metrics_filter_weekdays and days.upper() == "WEEKDAY":
        dates = weekday_only(dates)

    windows = time_windows(from_time, to_time, cfg.metrics_window_minutes)
    total_requests = len(dates) * len(windows)

    logger.info(
        "Fetching serviceMetrics in chunks: dates=%d windows_per_day=%d window_minutes=%d total_requests=%d",
        len(dates),
        len(windows),
        cfg.metrics_window_minutes,
        total_requests,
    )

    merged_services: list[dict] = []
    req_idx = 0

    for d in dates:
        for w_from, w_to in windows:
            req_idx += 1
            payload = {
                **base_payload,
                "from_date": d,
                "to_date": d,
                "from_time": w_from,
                "to_time": w_to,
            }
            logger.info("serviceMetrics chunk %d/%d date=%s %s-%s", req_idx, total_requests, d, w_from, w_to)
            mj = post_with_retry(cfg, client, "/serviceMetrics", payload)
            merged_services.extend(mj.get("Services", []) or [])

    logger.info("serviceMetrics chunks complete: merged_services=%d", len(merged_services))
    return merged_services


def extract_rids_and_templates(
    services: list[dict],
) -> tuple[list[str], dict[str, tuple[str, str, str]]]:
    """
    Returns:
      rids: unique list of rids
      templates: rid -> (gbtt_ptd, gbtt_pta, toc_code)
    """
    rids: list[str] = []
    templates: dict[str, tuple[str, str, str]] = {}

    for s in services:
        attrs = s.get("serviceAttributesMetrics", {}) or {}
        gbtt_ptd = (attrs.get("gbtt_ptd") or "").strip()
        gbtt_pta = (attrs.get("gbtt_pta") or "").strip()
        toc = (attrs.get("toc_code") or "").strip()

        for rid in as_list(attrs.get("rids")):
            if not rid:
                continue
            if rid in templates:
                continue
            rids.append(rid)
            templates[rid] = (gbtt_ptd, gbtt_pta, toc)

    return rids, templates
