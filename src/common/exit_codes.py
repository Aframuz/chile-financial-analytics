"""Process exit codes: the contract between entrypoints and the orchestrator.

    0   EXIT_SUCCESS   every series succeeded
    1   EXIT_FAILURE   permanent: quality failure, bad config, invalid
                       credentials, programming bug. Rerunning won't help.
                       (Also Python's default for an uncaught exception.)
    75  EXIT_TEMPFAIL  transient: network, timeout, 5xx, temporary
                       BigQuery/GCS outage. A rerun may succeed.
                       (EX_TEMPFAIL from sysexits.h.)

Airflow retries only EXIT_TEMPFAIL; anything else fails immediately.
"""

import logging
from collections.abc import Callable
from typing import Any

from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as google_auth_exceptions

from src.common.retry import is_transient

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_TEMPFAIL = 75

# Raised by GCS/BigQuery clients once their own built-in retries give up.
TRANSIENT_GOOGLE_ERRORS = (
    google_exceptions.TooManyRequests,
    google_exceptions.InternalServerError,
    google_exceptions.BadGateway,
    google_exceptions.ServiceUnavailable,
    google_exceptions.GatewayTimeout,
    google_exceptions.DeadlineExceeded,
    google_auth_exceptions.TransportError,
)


def is_retryable(exc: BaseException) -> bool:
    """Return True if rerunning the whole process could succeed.

    Unknown errors are treated as permanent: a bug should fail
    fast instead of being retried.
    """

    if isinstance(exc, TRANSIENT_GOOGLE_ERRORS):
        return True

    return isinstance(exc, Exception) and is_transient(exc)


def exit_code_for(results: list[dict[str, Any]]) -> int:
    """Exit code for a run, from its per-series results.

    Retryable only if every failure is retryable: with any permanent
    failure the run cannot succeed, so retrying just delays the alert.
    """

    failures = [
        result
        for result in results
        if result["status"] != "success"
    ]

    if not failures:
        return EXIT_SUCCESS

    if all(
        result.get("retryable", False)
        for result in failures
    ):
        return EXIT_TEMPFAIL

    return EXIT_FAILURE


def combine_exit_codes(exit_codes: list[int]) -> int:
    """One exit code for several steps: the worst one wins.

    Permanent beats transient beats success, matching exit_code_for.
    """

    failures = [code for code in exit_codes if code != EXIT_SUCCESS]

    if not failures:
        return EXIT_SUCCESS

    if all(code == EXIT_TEMPFAIL for code in failures):
        return EXIT_TEMPFAIL

    return EXIT_FAILURE


def exit_code_for_exception(exc: Exception) -> int:
    """Log an uncaught exception and map it to an exit code."""

    retryable = is_retryable(exc)

    logger.error(
        "Pipeline failed (%s)",
        "retryable" if retryable else "permanent",
        exc_info=exc,
    )

    return EXIT_TEMPFAIL if retryable else EXIT_FAILURE


def run_entrypoint(main: Callable[[], int]) -> int:
    """Run `main`, mapping uncaught exceptions to an exit code."""

    try:
        return main()

    except Exception as exc:
        return exit_code_for_exception(exc)
