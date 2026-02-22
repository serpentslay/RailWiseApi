import os
from dataclasses import dataclass


@dataclass(frozen=True)
class HspConfig:
    base_url: str
    username: str
    password: str

    connect_timeout: float
    write_timeout: float
    pool_timeout: float
    metrics_read_timeout: float
    details_read_timeout: float

    metrics_window_minutes: int
    metrics_filter_weekdays: bool

    delay: float
    max_details: int

    retries: int
    backoff_base: float
    progress_every: int


def load_config() -> HspConfig:
    username = os.getenv("HSP_USERNAME")
    password = os.getenv("HSP_PASSWORD")
    if not username or not password:
        raise RuntimeError("HSP_USERNAME/HSP_PASSWORD not set in backend/.env")

    return HspConfig(
        base_url=os.getenv("HSP_BASE_URL", "https://hsp-prod.rockshore.net/api/v1"),
        username=username,
        password=password,
        connect_timeout=float(os.getenv("HSP_CONNECT_TIMEOUT_SECONDS", "10")),
        write_timeout=float(os.getenv("HSP_WRITE_TIMEOUT_SECONDS", "30")),
        pool_timeout=float(os.getenv("HSP_POOL_TIMEOUT_SECONDS", "30")),
        metrics_read_timeout=float(os.getenv("HSP_METRICS_READ_TIMEOUT_SECONDS", "240")),
        details_read_timeout=float(os.getenv("HSP_DETAILS_READ_TIMEOUT_SECONDS", "60")),
        metrics_window_minutes=int(os.getenv("HSP_METRICS_WINDOW_MINUTES", "60")),
        metrics_filter_weekdays=os.getenv("HSP_METRICS_FILTER_WEEKDAYS", "1") == "1",
        delay=float(os.getenv("HSP_REQUEST_DELAY_SECONDS", "0.15")),
        max_details=int(os.getenv("HSP_MAX_DETAILS", "0")),
        retries=int(os.getenv("HSP_RETRIES", "6")),
        backoff_base=float(os.getenv("HSP_BACKOFF_BASE_SECONDS", "1.5")),
        progress_every=int(os.getenv("HSP_PROGRESS_EVERY", "50")),
    )
