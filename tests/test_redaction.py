import logging

import requests

from src.common.redaction import (
    RedactingFormatter,
    redact,
    safe_error_message,
)


URL = (
    "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"
    "?token=s3cr3t-T0KEN&firstdate=2020-01-01&function=GetSeries"
)


def test_redact_removes_token_value():

    redacted = redact(URL)

    assert "s3cr3t-T0KEN" not in redacted
    assert "token=***&firstdate=2020-01-01" in redacted


def test_safe_error_message_redacts_request_errors():

    exc = requests.ConnectionError(
        f"Max retries exceeded with url: {URL}"
    )

    assert "s3cr3t-T0KEN" not in safe_error_message(exc)


def test_formatter_redacts_tracebacks():

    formatter = RedactingFormatter(
        logging.Formatter("%(message)s")
    )

    try:
        raise requests.ConnectionError(URL)
    except requests.ConnectionError:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=0,
            msg="failed",
            args=(),
            exc_info=__import__("sys").exc_info(),
        )

    output = formatter.format(record)

    assert "Traceback" in output
    assert "s3cr3t-T0KEN" not in output
