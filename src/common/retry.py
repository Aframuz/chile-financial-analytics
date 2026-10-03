"""Retry transient BCCh API failures with exponential backoff.

Only network-level errors are retried. Invalid credentials, unknown
series or bad dates (bcchapi.ResponseException, 4xx) fail immediately:
retrying them only delays the failure.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

import requests

from src.common.redaction import safe_error_message

logger = logging.getLogger(__name__)

T = TypeVar("T")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class RetryStats:
    """Retries performed and seconds spent waiting, across calls."""

    retries: int = 0
    wait_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "retries": self.retries,
            "wait_seconds": round(self.wait_seconds, 3),
        }


def is_transient(exc: Exception) -> bool:
    """Return True for errors worth retrying."""

    if isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            # BCCh occasionally answers with an HTML error page
            requests.JSONDecodeError,
        ),
    ):
        return True

    if isinstance(exc, requests.HTTPError):
        return (
            exc.response is not None
            and exc.response.status_code in RETRYABLE_STATUS_CODES
        )

    return False


def call_with_retries(
    func: Callable[[], T],
    description: str,
    attempts: int = 3,
    base_delay_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    stats: RetryStats | None = None,
) -> T:
    """Call `func`, retrying transient errors with exponential backoff.

    Waits base_delay, 2*base_delay, ... between attempts. The last
    error is re-raised once attempts are exhausted. Retries and waits
    are added to `stats`, so run summaries show flaky API calls.
    """

    for attempt in range(1, attempts + 1):

        try:
            return func()

        except Exception as exc:

            if attempt == attempts or not is_transient(exc):
                raise

            delay = base_delay_seconds * 2 ** (attempt - 1)

            logger.warning(
                "Transient error on %s (attempt %s/%s): %s. "
                "Retrying in %.0fs",
                description,
                attempt,
                attempts,
                safe_error_message(exc),
                delay,
            )

            if stats is not None:
                stats.retries += 1
                stats.wait_seconds += delay

            sleep(delay)

    raise AssertionError("unreachable")
