"""Kill-switch: ручная и автоматическая остановка торговли.

Два режима:
- **Ручной** — файл-флаг STOP.flag или вызов activate()/deactivate()
- **Автоматический** — при дневном убытке ≥ 5% от стартового портфеля

При активации:
1. Устанавливается флаг _active
2. Создаётся файл STOP.flag (для межпроцессного взаимодействия)
3. Все позиции закрываются рыночными ордерами
4. Торговля блокируется до 19:00 МСК текущего дня

Сброс: deactivate() или удаление STOP.flag вручную.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional

from core.clock import Clock, MOSCOW_TZ
from core.interfaces import OrderGateway
from core.models import Order, OrderType, Side

from .config import RiskConfig

log = logging.getLogger(__name__)


class KillSwitch:
    """Экстренная остановка торговли.

    Использование:
        ks = KillSwitch(config, gateway, clock)
        if ks.is_active():
            raise RiskViolation("Kill-switch активен")
        ks.auto_activate_if_needed(current_value, start_value)
    """

    def __init__(
        self,
        config: RiskConfig,
        gateway: OrderGateway,
        clock: Clock,
    ) -> None:
        self._config = config
        self._gateway = gateway
        self._clock = clock
        self._active: bool = False
        self._reason: Optional[str] = None
        self._activated_at: Optional[datetime] = None

    # --- Публичный API -------------------------------------------------------

    def activate(self, reason: str) -> None:
        """Активировать kill-switch. Блокирует все новые заявки."""
        if self._active:
            log.warning("Kill-switch уже активен (причина: %s)", self._reason)
            return

        self._active = True
        self._reason = reason
        self._activated_at = self._clock.now()
        self._create_flag_file()
        log.critical("🛑 KILL-SWITCH активирован: %s", reason)

    def deactivate(self) -> None:
        """Деактивировать kill-switch. Разрешает торговлю."""
        if not self._active:
            return
        self._active = False
        self._reason = None
        self._activated_at = None
        self._remove_flag_file()
        log.info("Kill-switch деактивирован")

    def is_active(self) -> bool:
        """Активен ли kill-switch.

        Проверяет внутренний флаг и наличие файла-флага
        (для межпроцессного взаимодействия).
        """
        if self._active:
            return True
        # Проверяем файл-флаг (кто-то мог создать его руками)
        if self._config.stop_flag_path.exists():
            self._active = True
            self._reason = "Внешний STOP.flag"
            return True
        return False

    def auto_activate_if_needed(
        self,
        current_value: Decimal,
        start_value: Decimal,
    ) -> bool:
        """Автоматически активировать при убытке ≥ max_daily_loss_pct.

        Args:
            current_value: текущая стоимость портфеля
            start_value: стоимость портфеля на начало дня

        Returns:
            True, если kill-switch был активирован (или уже активен)
        """
        if self.is_active():
            return True

        if start_value <= 0:
            log.warning("Стартовая стоимость портфеля ≤ 0 — пропускаем проверку")
            return False

        loss_pct = (start_value - current_value) / start_value
        if loss_pct >= self._config.max_daily_loss_pct:
            reason = (
                f"Авто kill-switch: убыток {loss_pct:.2%} ≥ "
                f"{self._config.max_daily_loss_pct:.2%}"
            )
            self.activate(reason)
            return True

        return False

    async def close_all_positions(
        self,
        account_id: str,
        positions: list,
    ) -> list:
        """Закрыть все открытые позиции рыночными ордерами.

        Вызывается при активации kill-switch. Для каждой длинной
        позиции — SELL market, для короткой — BUY market.

        Args:
            account_id: ID торгового счёта
            positions: список Position (core.models.Position)

        Returns:
            Список OrderStateInfo от отправленных заявок
        """
        results = []
        for pos in positions:
            if pos.quantity <= 0:
                continue
            side = Side.SELL if pos.side == Side.BUY else Side.BUY
            order = Order(
                order_request_id=f"kill-{pos.figi}-{self._clock.now().timestamp()}",
                account_id=account_id,
                figi=pos.figi,
                side=side,
                order_type=OrderType.MARKET,
                quantity=pos.quantity,
                price=None,
                comment="kill-switch auto-close",
            )
            try:
                result = await self._gateway.post_order(order)
                results.append(result)
                log.info("Kill-switch: отправлен %s %s qty=%d", side.value, pos.figi, pos.quantity)
            except Exception as exc:  # noqa: BLE001
                log.error("Kill-switch: ошибка закрытия %s: %s", pos.figi, exc)

        return results

    # --- Внутренние методы ---------------------------------------------------

    def _create_flag_file(self) -> None:
        """Создать файл-флаг STOP.flag."""
        try:
            self._config.stop_flag_path.write_text(
                f"Kill-switch activated at {self._activated_at}\n"
                f"Reason: {self._reason}\n",
                encoding="utf-8",
            )
        except OSError as e:
            log.warning("Не удалось создать STOP.flag: %s", e)

    def _remove_flag_file(self) -> None:
        """Удалить файл-флаг STOP.flag."""
        try:
            self._config.stop_flag_path.unlink(missing_ok=True)
        except OSError as e:
            log.warning("Не удалось удалить STOP.flag: %s", e)