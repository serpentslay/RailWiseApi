import logging
import random
import time
from typing import Optional

import httpx

from .config import HspConfig

logger = logging.getLogger(__name__)

RETRY_STATUSES = {429, 502, 503, 504, 520, 522, 524}


def mask_basic_auth(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    parts = value.split(" ", 1)
    if len(parts) != 2:
        return "****"
    scheme, token = parts
    if len(token) <= 6:
        return f"{scheme} ****"
    return f"{scheme} ****{token[-4:]}"


def configure_logging_if_needed() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


def log_request(request: httpx.Request) -> None:
    logger.debug("HTTP %s %s", request.method, request.url)
    logger.debug("HTTP Authorization: %s", mask_basic_auth(request.headers.get("authorization")))


def make_client(cfg: HspConfig, *, read_timeout: float) -> httpx.Client:
    auth = httpx.BasicAuth(cfg.username, cfg.password)
    timeout = httpx.Timeout(
        connect=cfg.connect_timeout,
        read=read_timeout,
        write=cfg.write_timeout,
        pool=cfg.pool_timeout,
    )
    return httpx.Client(
        base_url=cfg.base_url,
        auth=auth,
        timeout=timeout,
        headers={"Content-Type": "application/json"},
        event_hooks={"request": [log_request]},
    )


def sleep_backoff(cfg: HspConfig, *, attempt: int, path: str) -> None:
    sleep_s = cfg.backoff_base * (2 ** (attempt - 1))
    sleep_s += random.uniform(0, 0.5)
    logger.info("Sleeping %.2fs before retrying %s", sleep_s, path)
    time.sleep(sleep_s)


def post_with_retry(cfg: HspConfig, client: httpx.Client, path: str, payload: dict) -> dict:
    last_err: Exception | None = None

    for attempt in range(1, cfg.retries + 1):
        t0 = time.perf_counter()
        try:
            r = client.post(path, json=payload)
            elapsed = time.perf_counter() - t0

            if r.status_code in RETRY_STATUSES:
                snippet = (r.text or "")[:300]
                logger.warning(
                    "Retryable HTTP %d (attempt %d/%d) POST %s after %.2fs body_snippet=%r",
                    r.status_code,
                    attempt,
                    cfg.retries,
                    path,
                    elapsed,
                    snippet,
                )
                raise httpx.HTTPStatusError("Retryable status", request=r.request, response=r)

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
                cfg.retries,
                path,
                elapsed,
                float(client.timeout.connect),
                float(client.timeout.read),
            )

        except httpx.HTTPStatusError as e:
            elapsed = time.perf_counter() - t0
            last_err = e
            status = e.response.status_code if e.response is not None else None
            if status not in RETRY_STATUSES:
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
                cfg.retries,
                path,
                elapsed,
                e,
            )

        sleep_backoff(cfg, attempt=attempt, path=path)

    raise last_err  # type: ignore
