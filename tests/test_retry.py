import pytest
import requests

from bcchapi import InvalidCredentials

from src.common.retry import (
    RetryStats,
    call_with_retries,
    is_transient,
)


def http_error(status_code):

    response = requests.Response()
    response.status_code = status_code

    return requests.HTTPError(response=response)


class FlakyCall:
    """Raise the given errors in order, then return 'ok'."""

    def __init__(self, errors):
        self.errors = list(errors)
        self.calls = 0

    def __call__(self):
        self.calls += 1

        if self.errors:
            raise self.errors.pop(0)

        return "ok"


def test_retries_transient_errors_until_success():

    func = FlakyCall(
        [
            requests.ConnectionError(),
            http_error(503),
        ]
    )
    delays = []

    result = call_with_retries(
        func,
        description="test",
        attempts=3,
        base_delay_seconds=2,
        sleep=delays.append,
    )

    assert result == "ok"
    assert func.calls == 3
    assert delays == [2, 4]


def test_reraises_after_exhausting_attempts():

    func = FlakyCall(
        [requests.Timeout()] * 3
    )

    with pytest.raises(requests.Timeout):
        call_with_retries(
            func,
            description="test",
            attempts=3,
            sleep=lambda _: None,
        )

    assert func.calls == 3


def test_does_not_retry_permanent_errors():

    func = FlakyCall(
        [InvalidCredentials("bad token")]
    )

    with pytest.raises(InvalidCredentials):
        call_with_retries(
            func,
            description="test",
            sleep=lambda _: None,
        )

    assert func.calls == 1


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (requests.ConnectionError(), True),
        (requests.Timeout(), True),
        (http_error(429), True),
        (http_error(502), True),
        (http_error(404), False),
        (InvalidCredentials("x"), False),
        (ValueError("x"), False),
    ],
)
def test_is_transient(exc, expected):

    assert is_transient(exc) is expected


def test_retry_stats_count_retries_and_waits():

    stats = RetryStats()

    call_with_retries(
        FlakyCall([requests.ConnectionError(), requests.Timeout()]),
        description="test",
        base_delay_seconds=2,
        sleep=lambda _: None,
        stats=stats,
    )

    assert stats.as_dict() == {"retries": 2, "wait_seconds": 6.0}
