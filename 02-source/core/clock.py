"""Источник времени для всего робота.

В интрадее важно единое время. Сейчас — простая обёртка над datetime с МСК.
В будущем можно подменить на серверное время из gRPC-ответов T-Invest (поля time),
чтобы не зависеть от локальных часов.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from .interfaces import Clock


MOSCOW_TZ = timezone(timedelta(hours=3))


class SystemClock(Clock):
    """Локальные системные часы (UTC+3)."""

    def now(self) -> datetime:
        return datetime.now(MOSCOW_TZ)

    @staticmethod
    def utc_now() -> datetime:
        """UTC — для логов и совместимости с ISO 8601."""
        return datetime.now(timezone.utc)


__all__ = ["Clock", "SystemClock", "MOSCOW_TZ"]
