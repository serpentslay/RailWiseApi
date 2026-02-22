import hashlib

def make_service_key(origin: str, destination: str, operator: str, service_date: str, sched_dep_iso: str) -> str:
    raw = f"{origin}|{destination}|{operator}|{service_date}|{sched_dep_iso}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
