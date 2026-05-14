"""
Axiom ingest for CPLS observability: unified JSON events (metrics + logs at write time).

emit_event is fire-and-forget and never raises. Configure AXIOM_TOKEN and AXIOM_DATASET to enable sends.
"""

import json
import logging
import os
from datetime import datetime, timezone
from time import time
from typing import Any, Dict, Optional

import httpx

from .config import ENVIRONMENT

_logging_initialized = False

_last_axiom_error_ts = 0

_axiom_client: Optional[httpx.AsyncClient] = None


def _get_axiom_client() -> httpx.AsyncClient:
    global _axiom_client
    if _axiom_client is None:
        _axiom_client = httpx.AsyncClient(timeout=httpx.Timeout(2.0))
    return _axiom_client


def _log_axiom_error_once(
    url: str,
    exception: Exception,
    status_code: Optional[int] = None,
    response_body: str = "",
) -> None:
    """Log Axiom ingest failure at most once per 60 seconds."""
    global _last_axiom_error_ts

    now = time()
    if now - _last_axiom_error_ts < 60:
        return

    _last_axiom_error_ts = now

    log = logging.getLogger("cpls.observability")
    extra_fields: Dict[str, Any] = {
        "endpoint": url or "unknown",
        "env": ENVIRONMENT,
        "exception_class": type(exception).__name__,
        "error": str(exception),
    }
    if status_code is not None:
        extra_fields["status_code"] = status_code
    if response_body:
        extra_fields["response_body"] = response_body

    log.warning("Axiom ingest failed", extra={"extra_fields": extra_fields})


async def emit_event(event_type: str, fields: dict) -> None:
    """POST one event to Axiom. Never raises; no-op if AXIOM_TOKEN or AXIOM_DATASET is unset."""
    url = "https://api.axiom.co/v1/datasets/<unset>/ingest"
    try:
        token = os.getenv("AXIOM_TOKEN", "")
        dataset = os.getenv("AXIOM_DATASET", "")
        if not token or not dataset:
            return

        url = f"https://api.axiom.co/v1/datasets/{dataset}/ingest"
        _time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        event: Dict[str, Any] = {
            "_time": _time,
            "event_type": event_type,
            "env": ENVIRONMENT,
            "service": "cpls",
            **fields,
        }

        client = _get_axiom_client()
        resp = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=[event],
        )
        if resp.is_error:
            body = (resp.text or "")[:300]
            _log_axiom_error_once(
                url,
                Exception(f"HTTP {resp.status_code}"),
                status_code=resp.status_code,
                response_body=body,
            )
    except Exception as e:
        try:
            _log_axiom_error_once(url, e)
        except Exception:
            pass


class JSONFormatter(logging.Formatter):
    """Formatter that outputs structured JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "env": ENVIRONMENT,
            "service": "cpls",
        }

        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            log_data.update(record.extra_fields)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging():
    """Configure root logger with JSON formatter. Only runs once to prevent handler duplication."""
    global _logging_initialized

    if _logging_initialized:
        return logging.getLogger()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    _logging_initialized = True
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Return a logger; does not configure handlers — call setup_logging() at app startup."""
    return logging.getLogger(name)
