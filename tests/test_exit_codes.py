import pytest
import requests

from bcchapi import InvalidCredentials
from google.api_core import exceptions as google_exceptions

from src.common.exit_codes import (
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_TEMPFAIL,
    exit_code_for,
    is_retryable,
    run_entrypoint,
)


SUCCESS = {"status": "success"}
QUALITY_FAILED = {"status": "quality_failed"}
TRANSIENT_FAILED = {"status": "technical_failed", "retryable": True}
PERMANENT_FAILED = {"status": "technical_failed", "retryable": False}


@pytest.mark.parametrize(
    ("results", "expected"),
    [
        ([SUCCESS, SUCCESS], EXIT_SUCCESS),
        ([SUCCESS, TRANSIENT_FAILED], EXIT_TEMPFAIL),
        ([TRANSIENT_FAILED, TRANSIENT_FAILED], EXIT_TEMPFAIL),
        ([SUCCESS, QUALITY_FAILED], EXIT_FAILURE),
        ([SUCCESS, PERMANENT_FAILED], EXIT_FAILURE),
        # A permanent failure means a retry cannot make the run succeed
        ([TRANSIENT_FAILED, QUALITY_FAILED], EXIT_FAILURE),
    ],
)
def test_exit_code_for(results, expected):

    assert exit_code_for(results) == expected


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (requests.ConnectionError(), True),
        (requests.Timeout(), True),
        (google_exceptions.ServiceUnavailable("x"), True),
        (google_exceptions.TooManyRequests("x"), True),
        (google_exceptions.BadRequest("invalid schema"), False),
        (google_exceptions.Forbidden("x"), False),
        (InvalidCredentials("x"), False),
        (RuntimeError("BCCH_API_TOKEN not configured"), False),
        (KeyError("bug"), False),
    ],
)
def test_is_retryable(exc, expected):

    assert is_retryable(exc) is expected


def test_run_entrypoint_returns_main_exit_code():

    assert run_entrypoint(lambda: EXIT_SUCCESS) == EXIT_SUCCESS


def raise_(exc):
    raise exc


def test_run_entrypoint_maps_transient_exception():

    assert run_entrypoint(
        lambda: raise_(requests.ConnectionError())
    ) == EXIT_TEMPFAIL


def test_run_entrypoint_maps_permanent_exception():

    assert run_entrypoint(
        lambda: raise_(ValueError("missing config"))
    ) == EXIT_FAILURE
