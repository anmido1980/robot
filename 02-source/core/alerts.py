"""Telegram-алерты для критических событий робота.

Использует HTTP-запросы к Telegram Bot API. Если TELEGRAM_BOT_TOKEN или
TELEGRAM_CHAT_ID не заданы — алерты логируются локально и не отправляются.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import aiohttp

log = logging.getLogger(__name__)


@dataclass
class TelegramConfig:
    """Конфигурация Telegram-алертов."""

    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    enabled: bool = False

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        """Загрузить конфигурацию из переменных окружения."""
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        enabled = bool(token) and bool(chat_id)
        return cls(bot_token=token, chat_id=chat_id, enabled=enabled)


class TelegramAlerter:
    """Отправка Telegram-уведомлений."""

    def __init__(self, config: Optional[TelegramConfig] = None) -> None:
        self.config = config or TelegramConfig.from_env()

    async def _send(self, message: str) -> bool:
        """Отправить сообщение в Telegram."""
        if not self.config.enabled:
            log.info(f"[ALERT] {message}")
            return False

        if not self.config.bot_token or not self.config.chat_id:
            log.info(f"[ALERT] {message}")
            return False

        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        payload = {
            "chat_id": self.config.chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning(f"Telegram API {resp.status}: {body}")
                        return False
                    log.debug("Telegram alert sent")
                    return True
        except Exception as e:  # noqa: BLE001
            log.warning(f"Ошибка отправки Telegram alert: {e}")
            return False

    # --- Специализированные алерты -------------------------------------------

    async def alert_kill_switch(self, reason: str, pnl: Decimal, capital: Decimal) -> bool:
        """Kill-switch активирован."""
        loss_pct = (-pnl / capital) if capital > 0 else Decimal(0)
        msg = (
            f"🛑 *Kill-switch активирован*\n"
            f"Причина: {reason}\n"
            f"Daily P&L: {pnl:,.2f} ({loss_pct:+.2%})\n"
            f"Торговля остановлена до следующего дня."
        )
        return await self._send(msg)

    async def alert_daily_loss_warning(self, pnl: Decimal, capital: Decimal) -> bool:
        """Дневной убыток превысил warning threshold."""
        loss_pct = (-pnl / capital) if capital > 0 else Decimal(0)
        msg = (
            f"⚠️ *Daily loss WARNING*\n"
            f"Daily P&L: {pnl:,.2f} ({loss_pct:+.2%})\n"
            f"Внимание: приближается к kill-switch."
        )
        return await self._send(msg)

    async def alert_heartbeat_stale(self, last_seen: Optional[str], stale_seconds: float) -> bool:
        """Heartbeat не обновлялся дольше порога."""
        msg = (
            f"🔥 *Heartbeat stale*\n"
            f"Последний: {last_seen or 'unknown'}\n"
            f"Простой: {stale_seconds:.0f} сек\n"
            f"Watchdog пытается рестартнуть runner."
        )
        return await self._send(msg)

    async def alert_crash_loop(self, attempts: int, last_error: str) -> bool:
        """Runner упал несколько раз подряд."""
        msg = (
            f"💥 *Crash loop*\n"
            f"Попыток: {attempts}\n"
            f"Последняя ошибка: {last_error[:200]}\n"
            f"Требуется ручная проверка."
        )
        return await self._send(msg)

    async def alert_backtest_divergence(
        self,
        paper_pnl: Decimal,
        expected_pnl: Decimal,
        divergence_pct: Decimal,
    ) -> bool:
        """Расхождение paper-P&L с бэктестом превысило порог."""
        msg = (
            f"📉 *Backtest divergence*\n"
            f"Paper P&L: {paper_pnl:,.2f}\n"
            f"Expected: {expected_pnl:,.2f}\n"
            f"Divergence: {divergence_pct:+.2%}\n"
            f"Требуется диагностика."
        )
        return await self._send(msg)
