import os
import time
import uuid
import logging
import random
from datetime import date as Date, datetime, timedelta
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.jobs.ingest.sources.base import BaseSource
from app.jobs.ingest.types import CanonicalServiceEvent
from app.jobs.ingest.utils.time import hhmm_to_dt, roll_if_next_day
from app.jobs.ingest.utils.service_key import make_service_key
from app.jobs.ingest.loader import load_events

# ---- Logging ----
logger = logging.getLogger(__name__)


def _configure_logging_if_needed() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


# ---- Helpers ----
def _as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _mask_basic_auth(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    parts = value.split(" ", 1)
    if len(parts) != 2:
        return "****"
    scheme, token = parts
    if len(token) <= 6:
        return f"{scheme} ****"
    return f"{scheme} ****{token[-4:]}"


def _parse_hhmm(hhmm: str) -> tuple[int, int]:
    hhmm = hhmm.strip()
    return int(hhmm[:2]), int(hhmm[2:])


def _fmt_hhmm(h: int, m: int) -> str:
    return f"{h:02d}{m:02d}"


def _time_windows(from_time: str, to_time: str, step_minutes: int) -> list[tuple[str, str]]:
    """
    Produce HHMM windows [start, end], stepping by step_minutes.
    Assumes from_time < to_time within same day.
    """
    sh, sm = _parse_hhmm(from_time)
    eh, em = _parse_hhmm(to_time)

    start = datetime(2000, 1, 1, sh, sm)
    end = datetime(2000, 1, 1, eh, em)

    windows: list[tuple[str, str]] = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(minutes=step_minutes), end)
        windows.append((_fmt_hhmm(cur.hour, cur.minute), _fmt_hhmm(nxt.hour, nxt.minute)))
        cur = nxt
    return windows


def _date_range(from_date: str, to_date: str) -> list[str]:
    d0 = Date.fromisoformat(from_date)
    d1 = Date.fromisoformat(to_date)
    out: list[str] = []
    d = d0
    while d <= d1:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _weekday_only(dates: list[str]) -> list[str]:
    """Filter ISO date strings to Mon–Fri."""
    out: list[str] = []
    for s in dates:
        d = Date.fromisoformat(s)
        if d.weekday() < 5:
            out.append(s)
    return out


class HspSource(BaseSource):
    """
    Darwin HSP ingestion:
      - POST /serviceMetrics to get RIDs
      - POST /serviceDetails per RID to get schedule + actuals at corridor endpoints
    """

    RETRY_STATUSES = {429, 502, 503, 504, 520, 522, 524}

    def __init__(self):
        _configure_logging_if_needed()

        self.base_url = os.getenv("HSP_BASE_URL", "https://hsp-prod.rockshore.net/api/v1")
        self.username = os.getenv("HSP_USERNAME")
        self.password = os.getenv("HSP_PASSWORD")

        if not self.username or not self.password:
            raise RuntimeError("HSP_USERNAME/HSP_PASSWORD not set in backend/.env")

        # Timeout tuning (separate read timeouts per endpoint)
        self.connect_timeout = float(os.getenv("HSP_CONNECT_TIMEOUT_SECONDS", "10"))
        self.write_timeout = float(os.getenv("HSP_WRITE_TIMEOUT_SECONDS", "30"))
        self.pool_timeout = float(os.getenv("HSP_POOL_TIMEOUT_SECONDS", "30"))
        self.metrics_read_timeout = float(os.getenv("HSP_METRICS_READ_TIMEOUT_SECONDS", "240"))
        self.details_read_timeout = float(os.getenv("HSP_DETAILS_READ_TIMEOUT_SECONDS", "60"))

        # Chunking to reduce serviceMetrics response size / upstream load
        self.metrics_window_minutes = int(os.getenv("HSP_METRICS_WINDOW_MINUTES", "60"))
        self.metrics_filter_weekdays = os.getenv("HSP_METRICS_FILTER_WEEKDAYS", "1") == "1"

        # Politeness & limits
        self.delay = float(os.getenv("HSP_REQUEST_DELAY_SECONDS", "0.15"))
        self.max_details = int(os.getenv("HSP_MAX_DETAILS", "0"))  # 0 = unlimited

        # Retry policy
        self.retries = int(os.getenv("HSP_RETRIES", "6"))
        self.backoff_base = float(os.getenv("HSP_BACKOFF_BASE_SECONDS", "1.5"))
        self.progress_every = int(os.getenv("HSP_PROGRESS_EVERY", "50"))

        logger.info(
            "HSP configured base_url=%s timeouts(connect=%.1f write=%.1f pool=%.1f read_metrics=%.1f read_details=%.1f) "
            "metrics_window_minutes=%d filter_weekdays=%s delay=%.2f retries=%d backoff_base=%.2f max_details=%s",
            self.base_url,
            self.connect_timeout,
            self.write_timeout,
            self.pool_timeout,
            self.metrics_read_timeout,
            self.details_read_timeout,
            self.metrics_window_minutes,
            self.metrics_filter_weekdays,
            self.delay,
            self.retries,
            self.backoff_base,
            "unlimited" if self.max_details == 0 else str(self.max_details),
        )

    @staticmethod
    def _log_request(request: httpx.Request):
        logger.debug("HTTP %s %s", request.method, request.url)
        logger.debug("HTTP Authorization: %s", _mask_basic_auth(request.headers.get("authorization")))

    def _client(self, read_timeout: float) -> httpx.Client:
        auth = httpx.BasicAuth(self.username, self.password)
        timeout = httpx.Timeout(
            connect=self.connect_timeout,
            read=read_timeout,
            write=self.write_timeout,
            pool=self.pool_timeout,
        )
        return httpx.Client(
            base_url=self.base_url,
            auth=auth,
            timeout=timeout,
            headers={"Content-Type": "application/json"},
            event_hooks={"request": [self._log_request]},
        )

    def _sleep_backoff(self, attempt: int, path: str) -> None:
        # Exponential backoff + jitter
        sleep_s = self.backoff_base * (2 ** (attempt - 1))
        sleep_s += random.uniform(0, 0.5)
        logger.info("Sleeping %.2fs before retrying %s", sleep_s, path)
        time.sleep(sleep_s)

    def _post_with_retry(self, client: httpx.Client, path: str, payload: dict) -> dict:
        last_err: Exception | None = None

        for attempt in range(1, self.retries + 1):
            t0 = time.perf_counter()
            try:
                r = client.post(path, json=payload)
                elapsed = time.perf_counter() - t0

                # Retryable gateway/rate-limit statuses
                if r.status_code in self.RETRY_STATUSES:
                    snippet = (r.text or "")[:300]
                    logger.warning(
                        "Retryable HTTP %d (attempt %d/%d) POST %s after %.2fs body_snippet=%r",
                        r.status_code,
                        attempt,
                        self.retries,
                        path,
                        elapsed,
                        snippet,
                    )
                    raise httpx.HTTPStatusError("Retryable status", request=r.request, response=r)

                # Normal status handling
                if elapsed > 10:
                    logger.info("POST %s completed in %.2fs status=%d (slow)", path, elapsed, r.status_code)
                else:
                    logger.debug("POST %s completed in %.2fs status=%d", path, elapsed, r.status_code)

                r.raise_for_status()
                return r.json()

            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                elapsed = time.perf_counter() - t0
                last_err = e
                logger.warning(
                    "%s (attempt %d/%d) POST %s after %.2fs (timeouts: connect=%.1fs read=%.1fs)",
                    e.__class__.__name__,
                    attempt,
                    self.retries,
                    path,
                    elapsed,
                    float(client.timeout.connect),
                    float(client.timeout.read),
                )

            except httpx.HTTPStatusError as e:
                elapsed = time.perf_counter() - t0
                last_err = e
                status = e.response.status_code if e.response is not None else None

                # Only retry statuses we consider transient
                if status not in self.RETRY_STATUSES:
                    snippet = (e.response.text or "")[:300] if e.response is not None else None
                    logger.error(
                        "Non-retryable HTTP %s POST %s after %.2fs body_snippet=%r",
                        status,
                        path,
                        elapsed,
                        snippet,
                    )
                    raise

            except Exception as e:
                elapsed = time.perf_counter() - t0
                last_err = e
                logger.warning(
                    "Request failed (attempt %d/%d) POST %s after %.2fs error=%r",
                    attempt,
                    self.retries,
                    path,
                    elapsed,
                    e,
                )

            self._sleep_backoff(attempt, path)

        raise last_err  # type: ignore

    def _fetch_service_metrics_chunked(
        self,
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
        """
        Calls /serviceMetrics in smaller chunks to avoid huge responses / 502s.
        Returns merged list of "Services" entries.
        """
        metrics_payload_base = {
            "from_loc": from_loc,
            "to_loc": to_loc,
            "days": days,
        }
        if toc_filter:
            metrics_payload_base["toc_filter"] = toc_filter

        dates = _date_range(from_date, to_date)

        # Optional: if caller asks WEEKDAY, skip weekend dates entirely (reduces calls)
        if self.metrics_filter_weekdays and days.upper() == "WEEKDAY":
            dates = _weekday_only(dates)

        windows = _time_windows(from_time, to_time, self.metrics_window_minutes)

        total_requests = len(dates) * len(windows)
        logger.info(
            "Fetching serviceMetrics in chunks: dates=%d windows_per_day=%d window_minutes=%d total_requests=%d",
            len(dates),
            len(windows),
            self.metrics_window_minutes,
            total_requests,
        )

        merged_services: list[dict] = []
        req_idx = 0
        for d in dates:
            for w_from, w_to in windows:
                req_idx += 1
                payload = {
                    **metrics_payload_base,
                    "from_date": d,
                    "to_date": d,
                    "from_time": w_from,
                    "to_time": w_to,
                }
                logger.info("serviceMetrics chunk %d/%d date=%s %s-%s", req_idx, total_requests, d, w_from, w_to)
                mj = self._post_with_retry(client, "/serviceMetrics", payload)
                merged_services.extend(mj.get("Services", []) or [])

        logger.info("serviceMetrics chunks complete: merged_services=%d", len(merged_services))
        return merged_services

    def ingest(
        self,
        db: Session,
        run_id: uuid.UUID,
        from_loc: str,
        to_loc: str,
        from_date: str,
        to_date: str,
        from_time: str,
        to_time: str,
        days: str,
        toc_filter: Optional[list[str]] = None,
    ) -> dict:
        """
        Args are HSP parameters:
          from_loc/to_loc: CRS codes (RDG, PAD, etc.)
          from_time/to_time: HHMM
          from_date/to_date: YYYY-MM-DD
          days: WEEKDAY | SATURDAY | SUNDAY
          toc_filter: optional list like ["GW"] etc
        """
        source_run_id = run_id  # reuse run_id for traceability

        logger.info(
            "HSP ingest start run_id=%s %s->%s dates=%s..%s times=%s..%s days=%s toc_filter=%s",
            str(run_id),
            from_loc,
            to_loc,
            from_date,
            to_date,
            from_time,
            to_time,
            days,
            toc_filter or [],
            )

        # ---- 1) serviceMetrics (chunked, longer read timeout) ----
        logger.info("Fetching serviceMetrics (chunked) read_timeout=%.1fs ...", self.metrics_read_timeout)
        with self._client(read_timeout=self.metrics_read_timeout) as metrics_client:
            services = self._fetch_service_metrics_chunked(
                metrics_client,
                from_loc=from_loc,
                to_loc=to_loc,
                from_date=from_date,
                to_date=to_date,
                from_time=from_time,
                to_time=to_time,
                days=days,
                toc_filter=toc_filter,
            )

        rids: list[str] = []
        service_templates: dict[str, tuple[str, str, str]] = {}  # rid -> (gbtt_ptd, gbtt_pta, toc)

        for s in services:
            attrs = s.get("serviceAttributesMetrics", {}) or {}
            gbtt_ptd = (attrs.get("gbtt_ptd") or "").strip()  # HHMM at origin
            gbtt_pta = (attrs.get("gbtt_pta") or "").strip()  # HHMM at destination
            toc = (attrs.get("toc_code") or "").strip()
            rid_list = _as_list(attrs.get("rids"))

            for rid in rid_list:
                if not rid:
                    continue
                # De-dupe (chunking can yield duplicates)
                if rid in service_templates:
                    continue
                rids.append(rid)
                service_templates[rid] = (gbtt_ptd, gbtt_pta, toc)

        logger.info("serviceMetrics produced %d unique RIDs (from merged_services=%d)", len(rids), len(services))

        # ---- 2) serviceDetails per RID (shorter read timeout, many calls) ----
        events: list[CanonicalServiceEvent] = []
        details_fetched = 0
        details_failed = 0
        invalid_skipped = 0

        total = len(rids)
        logger.info("Fetching serviceDetails for %d RIDs read_timeout=%.1fs ...", total, self.details_read_timeout)

        with self._client(read_timeout=self.details_read_timeout) as details_client:
            for idx, rid in enumerate(rids, start=1):
                if self.max_details and idx > self.max_details:
                    logger.info(
                        "Stopping early due to HSP_MAX_DETAILS=%d (processed=%d/%d)",
                        self.max_details,
                        idx - 1,
                        total,
                        )
                    break

                if self.progress_every and (idx == 1 or idx % self.progress_every == 0 or idx == total):
                    logger.info(
                        "serviceDetails progress %d/%d (fetched=%d failed=%d skipped=%d)",
                        idx,
                        total,
                        details_fetched,
                        details_failed,
                        invalid_skipped,
                    )

                if self.delay > 0:
                    time.sleep(self.delay)

                try:
                    details_json = self._post_with_retry(details_client, "/serviceDetails", {"rid": rid})
                    data = details_json.get("serviceAttributesDetails", {}) or {}

                    dos = (data.get("date_of_service") or "").strip()  # YYYY-MM-DD
                    if not dos:
                        invalid_skipped += 1
                        logger.debug("RID %s skipped: missing date_of_service", rid)
                        continue

                    service_date = Date.fromisoformat(dos)

                    toc = (data.get("toc_code") or "").strip()
                    if not toc:
                        toc = service_templates.get(rid, ("", "", ""))[2]

                    locs = data.get("locations", []) or []
                    origin_row = next((x for x in locs if x.get("location") == from_loc), None)
                    dest_row = next((x for x in locs if x.get("location") == to_loc), None)

                    if not origin_row or not dest_row:
                        invalid_skipped += 1
                        logger.debug(
                            "RID %s skipped: missing origin/destination rows for %s/%s",
                            rid,
                            from_loc,
                            to_loc,
                        )
                        continue

                    # Scheduled times (fallback to metrics values)
                    gbtt_ptd = (origin_row.get("gbtt_ptd") or "").strip() or service_templates.get(rid, ("", "", ""))[0]
                    gbtt_pta = (dest_row.get("gbtt_pta") or "").strip() or service_templates.get(rid, ("", "", ""))[1]

                    sched_dep = hhmm_to_dt(service_date, gbtt_ptd)
                    if sched_dep is None:
                        invalid_skipped += 1
                        logger.debug("RID %s skipped: invalid scheduled departure gbtt_ptd=%r", rid, gbtt_ptd)
                        continue

                    sched_arr = hhmm_to_dt(service_date, gbtt_pta)
                    sched_arr = roll_if_next_day(sched_dep, sched_arr)

                    # Actual arrival at destination
                    actual_ta = (dest_row.get("actual_ta") or "").strip()
                    act_arr = hhmm_to_dt(service_date, actual_ta)
                    act_arr = roll_if_next_day(sched_dep, act_arr)

                    # MVP cancellation heuristic:
                    cancelled = (act_arr is None)

                    delay_min = None
                    if (not cancelled) and sched_arr and act_arr:
                        delay_min = int(round((act_arr - sched_arr).total_seconds() / 60.0))

                    service_key = make_service_key(
                        origin=from_loc,
                        destination=to_loc,
                        operator=toc,
                        service_date=dos,
                        sched_dep_iso=sched_dep.isoformat(),
                    )

                    events.append(
                        CanonicalServiceEvent(
                            source="hsp",
                            source_event_id=rid,
                            service_date=dos,
                            operator=toc,
                            origin=from_loc,
                            destination=to_loc,
                            scheduled_departure_ts=sched_dep,
                            scheduled_arrival_ts=sched_arr,
                            actual_arrival_ts=act_arr,
                            cancelled=cancelled,
                            arrival_delay_minutes=delay_min,
                            service_key=service_key,
                        )
                    )
                    details_fetched += 1

                except Exception as e:
                    details_failed += 1
                    logger.exception("RID %s failed: %r", rid, e)
                    continue

        logger.info("Loading %d events into DB...", len(events))
        load_stats = load_events(db, events, source_run_id)
        logger.info("Load complete: %s", load_stats)

        result = {
            "source": "hsp",
            "from_loc": from_loc,
            "to_loc": to_loc,
            "from_date": from_date,
            "to_date": to_date,
            "from_time": from_time,
            "to_time": to_time,
            "days": days,
            "toc_filter": toc_filter or [],
            "rids_total": len(rids),
            "details_fetched": details_fetched,
            "details_failed": details_failed,
            "invalid_skipped": invalid_skipped,
            **load_stats,
        }

        logger.info("HSP ingest done run_id=%s result=%s", str(run_id), result)
        return result
