"""
app/middleware/request_id.py  —  Request-ID ASGI middleware + log filter.

Every incoming HTTP request gets a unique X-Request-ID header (or reuses
one provided by an upstream proxy).  The ID is stored in a ContextVar so
all log records emitted during that request carry a `request_id` field
automatically — no manual threading required.

Usage (wired once in main.py):
    app.add_middleware(RequestIDMiddleware)
    logging.config.dictConfig(LOGGING_CONFIG)   # see get_logging_config()
"""
import logging
import uuid
from contextvars import ContextVar
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Module-level ContextVar that survives async context switches
_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")


def get_request_id() -> str:
    """Return the request ID for the current async context (empty string if none)."""
    return _request_id_ctx.get()


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that:
    1. Reads X-Request-ID from the incoming request (or generates a new UUID4).
    2. Stores it in _request_id_ctx so all log statements in the same request
       automatically include it.
    3. Echoes the ID back in the response header X-Request-ID.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        token = _request_id_ctx.set(request_id)
        try:
            response: Response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            _request_id_ctx.reset(token)


class RequestIDLogFilter(logging.Filter):
    """
    Log filter that injects `request_id` into every LogRecord.
    Attach to the root logger handler so ALL log output is enriched.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.request_id = get_request_id() or "-"
        return True


def get_logging_config(level: str = "INFO") -> dict:
    """
    Return a dictConfig-compatible logging configuration that:
    - Formats every log line with timestamp, level, request_id, logger name, and message.
    - Attaches RequestIDLogFilter to the root handler.

    Call logging.config.dictConfig(get_logging_config()) early in main.py.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "request_id": {
                "()": "app.middleware.request_id.RequestIDLogFilter",
            }
        },
        "formatters": {
            "structured": {
                "format": (
                    "%(asctime)s %(levelname)-8s [%(request_id)s] %(name)s — %(message)s"
                ),
                "datefmt": "%Y-%m-%dT%H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "structured",
                "filters": ["request_id"],
            }
        },
        "root": {
            "level": level,
            "handlers": ["console"],
        },
        # Silence noisy third-party libs
        "loggers": {
            "uvicorn.access": {"level": "WARNING"},
            "pycelonis": {"level": "WARNING"},
            "httpx": {"level": "WARNING"},
        },
    }
