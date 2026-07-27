from __future__ import annotations

from collections import Counter

import structlog

logger = structlog.get_logger(__name__)

_counters: Counter[str] = Counter()


def incr(metric: str, *, amount: int = 1) -> None:
    _counters[metric] += amount


def get_count(metric: str) -> int:
    return int(_counters[metric])


def reset_for_tests() -> None:
    _counters.clear()


def emit_ops_alert(alert_code: str, **fields: object) -> None:
    incr(f"alert.{alert_code}")
    logger.error("ops.alert", alert_code=alert_code, **fields)
