"""Deployment-only bounded health gate for the signal observer."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from typing import Any


POLL_INTERVAL_SECONDS = 5.0
MAX_ADDITIONAL_WAIT_SECONDS = 120.0
MAX_HEARTBEAT_AGE_SECONDS = 90.0


class ObserverHealthGateError(RuntimeError):
    """Observer가 bounded deployment gate를 통과하지 못했다."""


def _age_seconds(value: Any, now_epoch: float) -> float | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        return None
    return now_epoch - float(value)


def _diagnostic(
    metrics: Mapping[str, Any],
    *,
    waited_seconds: float,
    now_epoch: float,
) -> str:
    heartbeat_age = _age_seconds(
        metrics.get("observer_heartbeat_at"), now_epoch
    )
    started_age = _age_seconds(metrics.get("observer_started_at"), now_epoch)

    def display(value: float | None) -> str:
        return "UNKNOWN" if value is None else f"{value:.1f}"

    return (
        f"state={metrics.get('observer_state', 'UNKNOWN')} "
        f"health_state={metrics.get('observer_health_state', 'LEGACY')} "
        f"waited_seconds={waited_seconds:.1f} "
        f"heartbeat_age_seconds={display(heartbeat_age)} "
        f"observer_started_age_seconds={display(started_age)} "
        f"last_error_type={metrics.get('observer_last_error_type', 'NONE')} "
        f"last_error_at={metrics.get('observer_last_error_at', 'NONE')}"
    )


def wait_for_observer_health(
    read_metrics: Callable[[], Mapping[str, Any]],
    *,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    max_additional_wait_seconds: float = MAX_ADDITIONAL_WAIT_SECONDS,
    max_heartbeat_age_seconds: float = MAX_HEARTBEAT_AGE_SECONDS,
    now: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    report: Callable[[str], None] = print,
) -> Mapping[str, Any]:
    """STARTING만 bounded poll하고 다른 비정상 상태는 즉시 실패한다."""
    if poll_interval_seconds <= 0:
        raise ValueError("poll interval must be positive")
    if max_additional_wait_seconds < 0:
        raise ValueError("maximum additional wait must be non-negative")

    started = monotonic()
    while True:
        metrics = read_metrics()
        waited = max(0.0, monotonic() - started)
        now_epoch = now()
        state = metrics.get("observer_state")
        diagnostic = _diagnostic(
            metrics,
            waited_seconds=waited,
            now_epoch=now_epoch,
        )
        report(f"OBSERVER_GATE_POLL {diagnostic}")

        if state == "RUNNING":
            health_state = metrics.get("observer_health_state")
            if health_state not in (None, "RUNNING_HEALTHY"):
                raise ObserverHealthGateError(
                    f"observer health state is not healthy: {diagnostic}"
                )
            heartbeat_age = _age_seconds(
                metrics.get("observer_heartbeat_at"), now_epoch
            )
            if (
                heartbeat_age is None
                or heartbeat_age > max_heartbeat_age_seconds
            ):
                raise ObserverHealthGateError(
                    f"observer heartbeat is missing or stale: {diagnostic}"
                )
            return metrics

        if state != "STARTING":
            raise ObserverHealthGateError(
                f"observer health gate failed: {diagnostic}"
            )

        if waited >= max_additional_wait_seconds:
            raise ObserverHealthGateError(
                f"observer STARTING timed out: {diagnostic}"
            )

        sleep(min(
            poll_interval_seconds,
            max_additional_wait_seconds - waited,
        ))
