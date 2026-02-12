"""
Minimal Datadog instrumentation for CPLS job lifecycle observability.

This module provides safe, fire-and-forget metric emission and structured JSON logging.
All operations are designed to never crash the application if Datadog is unavailable.
"""

import logging
import json
import os
import urllib.request
import urllib.error
from typing import Dict, Optional, List
from datetime import datetime
from time import time

from .config import ENVIRONMENT

# Logging initialization flag
_logging_initialized = False

# Rate limiting for metric error logs (once per 60 seconds)
_last_http_error_ts = 0


def _log_metric_error_once(metric_name: str, url: str, exception: Exception, status_code: Optional[int] = None, response_body: str = ""):
    """Log metric submission error at most once per 60 seconds. Rate-limited to prevent log flooding."""
    global _last_http_error_ts
    
    current_time = time()
    if current_time - _last_http_error_ts < 60:
        return
    
    _last_http_error_ts = current_time
    
    logger = get_logger("cpls.metrics")
    extra_fields = {
        "endpoint": url or "unknown",
        "env": ENVIRONMENT,
        "metric_name": metric_name,
        "exception_class": type(exception).__name__,
        "error": str(exception)
    }
    if status_code is not None:
        extra_fields["status_code"] = status_code
    if response_body:
        extra_fields["response_body"] = response_body
    
    logger.warning("Datadog metric submission failed", extra={"extra_fields": extra_fields})


def _send_metric_via_api(base_url: str, api_key: str, metric_name: str, value: float, metric_type: str, tags: List[str]):
    """Send a single metric via Datadog HTTP API v2. Fire-and-forget, never raises exceptions."""
    url = None
    try:
        # Map metric_type to Datadog v2 type enum (int)
        # 0 = unspecified, 1 = count, 2 = rate, 3 = gauge
        if metric_type == "count":
            type_enum = 1
        elif metric_type == "gauge":
            type_enum = 3
        elif metric_type == "histogram":
            type_enum = 3  # Treat histogram as gauge
        else:
            type_enum = 0  # unspecified
        
        # Current Unix timestamp in seconds
        timestamp = int(time())
        
        # Build payload according to Datadog v2 series API
        payload = {
            "series": [{
                "metric": metric_name,
                "type": type_enum,
                "points": [{"timestamp": timestamp, "value": value}],
                "tags": tags
            }]
        }
        
        # Create request
        url = f"{base_url}/api/v2/series"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "DD-API-KEY": api_key,
                "Content-Type": "application/json"
            },
            method="POST"
        )
        
        # Send with short timeout (2 seconds) to avoid blocking
        urllib.request.urlopen(req, timeout=2)
    except urllib.error.HTTPError as e:
        # HTTP error with status code and response body
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        
        _log_metric_error_once(metric_name, url, e, status_code=e.code, response_body=body)
    except Exception as e:
        # Any other exception
        _log_metric_error_once(metric_name, url, e)


def emit_job_metric(name: str, value: float, tags: Optional[Dict[str, str]] = None, metric_type: str = "count"):
    """
    Emit a job lifecycle metric to Datadog via HTTP API.
    
    Args:
        name: Metric name (will be prefixed with "cpls.job.")
        value: Metric value
        tags: Additional tags (infra_dao_slug, job_type will be added automatically)
        metric_type: One of "count", "gauge", "histogram"
    
    This function never raises exceptions and silently fails if Datadog is unavailable.
    Requires DD_API_KEY environment variable to be set. If missing, no-op.
    """
    try:
        # Read API key at call time (not import time)
        api_key = os.getenv("DD_API_KEY", "")
        
        # Skip if API key not configured (safe no-op)
        if not api_key:
            return
        
        # Read DD_SITE at call time (not import time)
        dd_site = os.getenv("DD_SITE", "datadoghq.com")
        base_url = f"https://api.{dd_site}"
        
        # Build full metric name with prefix
        full_name = f"cpls.job.{name}"
        
        # Build tags list with required tags
        tag_list = [
            f"env:{ENVIRONMENT}",
            "service:cpls"
        ]
        
        # Add custom tags
        if tags:
            for key, val in tags.items():
                if val is not None:  # Include zero/false values, only skip None
                    tag_list.append(f"{key}:{val}")
        
        # Send via HTTP API
        _send_metric_via_api(base_url, api_key, full_name, value, metric_type, tag_list)
    except Exception:
        # Silently swallow all errors - metrics should never break the app
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
        
        # Add extra fields if present
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            log_data.update(record.extra_fields)
        
        # Add exception info if present
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
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add JSON formatter handler
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    
    _logging_initialized = True
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger with structured JSON output."""
    # Ensure logging is set up (only once)
    setup_logging()
    return logging.getLogger(name)
