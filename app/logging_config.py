from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime


class JsonEventFormatter(logging.Formatter):
    """Emit a fixed set of fields so request bodies and exception messages are not logged."""

    _fields = (
        "request_id", "transaction_id", "attempt", "duration_ms", "status_code",
        "method", "route", "error_type", "worker_mode",
    )

    def format(self, record: logging.LogRecord) -> str:
        event = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for field in self._fields:
            value = getattr(record, field, None)
            if value is not None:
                event[field] = value
        return json.dumps(event, ensure_ascii=True, separators=(",", ":"))


def configure_logging() -> None:
    """Configure only this application's logger, leaving server logging alone."""
    logger = logging.getLogger("webhook")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not any(getattr(handler, "_webhook_json_handler", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonEventFormatter())
        handler._webhook_json_handler = True
        logger.addHandler(handler)


def error_type(exc: BaseException) -> str:
    """Record an exception class, never its message or chained database URL."""
    return type(exc.__cause__ or exc).__name__
