"""Keep the BCCh API token out of logs and run summaries.

bcchapi sends the token as a URL query parameter, so request errors
(and their tracebacks) carry it in the URL.
"""

import logging
import re

TOKEN_PATTERN = re.compile(r"(token=)[^&\s'\"]+", re.IGNORECASE)


def redact(text: str) -> str:
    """Replace token values in `text` with ***."""

    return TOKEN_PATTERN.sub(r"\1***", text)


def safe_error_message(exc: BaseException) -> str:
    """Return the exception message with tokens redacted."""

    return redact(str(exc))


class RedactingFormatter(logging.Formatter):
    """Redact the fully formatted record, traceback included."""

    def __init__(self, inner: logging.Formatter):
        super().__init__()
        self.inner = inner

    def format(self, record: logging.LogRecord) -> str:
        return redact(self.inner.format(record))


def redact_log_output() -> None:
    """Wrap every root handler's formatter with redaction."""

    for handler in logging.getLogger().handlers:

        if not isinstance(handler.formatter, RedactingFormatter):
            handler.setFormatter(
                RedactingFormatter(
                    handler.formatter or logging.Formatter()
                )
            )
