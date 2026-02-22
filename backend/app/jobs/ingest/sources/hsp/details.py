import logging
from datetime import date as Date
from typing import Optional

from app.jobs.ingest.types import CanonicalServiceEvent
from app.jobs.ingest.utils.service_key import make_service_key
from app.jobs.ingest.utils.time import hhmm_to_dt, roll_if_next_day

logger = logging.getLogger(__name__)


def details_to_event(
    *,
    rid: str,
    details_json: dict,
    from_loc: str,
    to_loc: str,
    service_templates: dict[str, tuple[str, str, str]],
) -> Optional[CanonicalServiceEvent]:
    data = details_json.get("serviceAttributesDetails", {}) or {}

    dos = (data.get("date_of_service") or "").strip()
    if not dos:
        logger.debug("RID %s skipped: missing date_of_service", rid)
        return None

    service_date = Date.fromisoformat(dos)

    toc = (data.get("toc_code") or "").strip() or service_templates.get(rid, ("", "", ""))[2]

    locs = data.get("locations", []) or []
    origin_row = next((x for x in locs if x.get("location") == from_loc), None)
    dest_row = next((x for x in locs if x.get("location") == to_loc), None)

    if not origin_row or not dest_row:
        logger.debug("RID %s skipped: missing origin/destination rows for %s/%s", rid, from_loc, to_loc)
        return None

    gbtt_ptd = (origin_row.get("gbtt_ptd") or "").strip() or service_templates.get(rid, ("", "", ""))[0]
    gbtt_pta = (dest_row.get("gbtt_pta") or "").strip() or service_templates.get(rid, ("", "", ""))[1]

    sched_dep = hhmm_to_dt(service_date, gbtt_ptd)
    if sched_dep is None:
        logger.debug("RID %s skipped: invalid scheduled departure gbtt_ptd=%r", rid, gbtt_ptd)
        return None

    sched_arr = roll_if_next_day(sched_dep, hhmm_to_dt(service_date, gbtt_pta))

    actual_ta = (dest_row.get("actual_ta") or "").strip()
    act_arr = roll_if_next_day(sched_dep, hhmm_to_dt(service_date, actual_ta))

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

    return CanonicalServiceEvent(
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
