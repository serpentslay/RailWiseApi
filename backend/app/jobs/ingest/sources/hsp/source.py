import logging
import time
import uuid
from typing import Optional

from sqlalchemy.orm import Session

from app.jobs.ingest.loader import load_events
from app.jobs.ingest.sources.base import BaseSource
from app.jobs.ingest.types import CanonicalServiceEvent

from .config import load_config
from .details import details_to_event
from .http import configure_logging_if_needed, make_client, post_with_retry
from .metrics import extract_rids_and_templates, fetch_service_metrics_chunked

logger = logging.getLogger(__name__)


class HspSource(BaseSource):
    """
    Darwin HSP ingestion:
      - POST /serviceMetrics to get RIDs
      - POST /serviceDetails per RID to get schedule + actuals at corridor endpoints
    """

    def __init__(self):
        configure_logging_if_needed()
        self.cfg = load_config()

        logger.info(
            "HSP configured base_url=%s timeouts(connect=%.1f write=%.1f pool=%.1f read_metrics=%.1f read_details=%.1f) "
            "metrics_window_minutes=%d filter_weekdays=%s delay=%.2f retries=%d backoff_base=%.2f max_details=%s",
            self.cfg.base_url,
            self.cfg.connect_timeout,
            self.cfg.write_timeout,
            self.cfg.pool_timeout,
            self.cfg.metrics_read_timeout,
            self.cfg.details_read_timeout,
            self.cfg.metrics_window_minutes,
            self.cfg.metrics_filter_weekdays,
            self.cfg.delay,
            self.cfg.retries,
            self.cfg.backoff_base,
            "unlimited" if self.cfg.max_details == 0 else str(self.cfg.max_details),
        )

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
        source_run_id = run_id

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

        # 1) serviceMetrics (chunked)
        logger.info("Fetching serviceMetrics (chunked) read_timeout=%.1fs ...", self.cfg.metrics_read_timeout)
        with make_client(self.cfg, read_timeout=self.cfg.metrics_read_timeout) as metrics_client:
            services = fetch_service_metrics_chunked(
                self.cfg,
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

        rids, service_templates = extract_rids_and_templates(services)
        logger.info("serviceMetrics produced %d unique RIDs (from merged_services=%d)", len(rids), len(services))

        # 2) serviceDetails per RID
        events: list[CanonicalServiceEvent] = []
        details_fetched = 0
        details_failed = 0
        invalid_skipped = 0

        total = len(rids)
        logger.info("Fetching serviceDetails for %d RIDs read_timeout=%.1fs ...", total, self.cfg.details_read_timeout)

        with make_client(self.cfg, read_timeout=self.cfg.details_read_timeout) as details_client:
            for idx, rid in enumerate(rids, start=1):
                if self.cfg.max_details and idx > self.cfg.max_details:
                    logger.info(
                        "Stopping early due to HSP_MAX_DETAILS=%d (processed=%d/%d)",
                        self.cfg.max_details,
                        idx - 1,
                        total,
                        )
                    break

                if self.cfg.progress_every and (idx == 1 or idx % self.cfg.progress_every == 0 or idx == total):
                    logger.info(
                        "serviceDetails progress %d/%d (fetched=%d failed=%d skipped=%d)",
                        idx,
                        total,
                        details_fetched,
                        details_failed,
                        invalid_skipped,
                    )

                if self.cfg.delay > 0:
                    time.sleep(self.cfg.delay)

                try:
                    details_json = post_with_retry(self.cfg, details_client, "/serviceDetails", {"rid": rid})
                    evt = details_to_event(
                        rid=rid,
                        details_json=details_json,
                        from_loc=from_loc,
                        to_loc=to_loc,
                        service_templates=service_templates,
                    )

                    if evt is None:
                        invalid_skipped += 1
                        continue

                    events.append(evt)
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
