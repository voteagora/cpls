"""
Minimal Datadog instrumentation for CPLS job lifecycle observability.

This module provides safe, fire-and-forget metric emission and structured JSON logging.
All operations are designed to never crash the application if Datadog is unavailable.
"""

import logging
import json
import os
from typing import Dict, Optional, Any
from datetime import datetime

from datadog import initialize, statsd
from .config import ENVIRONMENT

# Initialize Datadog client (safe, won't crash if agent not running)
_statsd_client = None
_initialized = False

# Logging initialization flag
_logging_initialized = False


def _ensure_initialized():
    """Lazily initialize Datadog client. Safe if agent not available."""
    global _statsd_client, _initialized
    
    if _initialized:
        return _statsd_client
    
    try:
        # Initialize with environment variable support for Railway/container deployments
        # DD_AGENT_HOST and DD_DOGSTATSD_PORT are standard Datadog environment variables
        statsd_host = os.getenv("DD_AGENT_HOST", "127.0.0.1")
        statsd_port = int(os.getenv("DD_DOGSTATSD_PORT", "8125"))
        initialize(statsd_host=statsd_host, statsd_port=statsd_port)
        _statsd_client = statsd
        _initialized = True
    except Exception:
        # If initialization fails, we continue without metrics
        # This ensures the app works even if Datadog is unavailable
        _statsd_client = None
        _initialized = True
    
    return _statsd_client


def emit_job_metric(name: str, value: float, tags: Optional[Dict[str, str]] = None, metric_type: str = "count"):
    """
    Emit a job lifecycle metric to Datadog.
    
    Args:
        name: Metric name (will be prefixed with "cpls.job.")
        value: Metric value
        tags: Additional tags (infra_dao_slug, job_type will be added automatically)
        metric_type: One of "count", "gauge", "histogram"
    
    This function never raises exceptions and silently fails if Datadog is unavailable.
    """
    try:
        client = _ensure_initialized()
        if client is None:
            return
        
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
        
        # Emit metric based on type
        if metric_type == "count":
            client.increment(full_name, value, tags=tag_list)
        elif metric_type == "gauge":
            client.gauge(full_name, value, tags=tag_list)
        elif metric_type == "histogram":
            client.histogram(full_name, value, tags=tag_list)
        else:
            # Unknown metric type, silently ignore
            pass
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
