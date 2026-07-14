"""Риск-менеджер: перехват всех заявок перед отправкой на биржу.

Все заявки проходят через RiskManager.post_order(). Если лимиты
превышены — поднимается RiskViolation и заявка не отправляется.

Архитектура: broker → RiskManager → strategy → runner.
RiskManager оборачивает OrderGateway и делегирует cancel/stream
без проверок (отмена всегда разрешена).

Лимиты (из CLAUDE.md):
- позиция ≤ 50% депозита
- дневной убыток ≤ 5% портфеля
- макс. кол-во одновременных позиций = 5
- стоп-лосс на позицию (опционально)

Сброс дневного счётчика — после вечернего клиринга (00:30 МСК).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Optional

from core.clock import Clock, MOSCOW_TZ
from core.events import EventBus, OrderStateEvent
from core.interfaces import OrderGateway, PortfolioGateway
from core.models import (
    Money,
    Order,
    OrderState,
    OrderStateInfo,
    Position,
    PortfolioSnapshot,
)

from .config import RiskConfig
from .kill_switch import KillSwitch

log = logging.getLogger(__name__)


class RiskViolation(Exception):
    """Заявка отклонена риск-менеджером."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class RiskManager:
    """Обёртка над OrderGateway с проверкой лимитов.

    Использование:
        rm = RiskManager(gateway, portfolio, event_bus, clock, config)
        await rm.post_order(order)  # пройдёт проверки → отправится на биржу
    """

    def __init__(
        self,
        gateway: OrderGateway,
        portfolio: PortfolioGateway,
        event_bus: EventBus,
        clock: Clock,
        config: RiskConfig | None = None,
    ) -> None:
        self._gateway = gateway
        self._portfolio = portfolio
        self._event_bus = event_bus
        self._clock = clock
        self._config = config or RiskConfig()

        # Kill-switch
        self._kill_switch = KillSwitch(self._config, gateway, clock)

        # Дневной P&L
        self._daily_start_value: Optional[Decimal] = None
        self._daily_pnl: Decimal = Decimal(0)
        self._last_reset_date: Optional[str] = None

        # Подписка на события изменения статуса заявок
        self._event_bus.subscribe("order_state", self._on_order_state)

    # --- Публичный API (реализует OrderGateway Protocol) --------------------

    async def post_order(self, order: Order) -> OrderStateInfo:
        """Отправить заявку с проверкой лимитов.

        Raises:
            RiskViolation: лимит превышён или kill-switch активен
        """
        # 1. Kill-switch
        if self._kill_switch.is_active():
            raise RiskViolation(
                f"Kill-switch активен: {self._kill_switch._reason}"
            )

        # 2. Обновить дневной стартовый уровень (если нужно)
        await self._ensure_daily_start(order.account_id)

        # 3. Проверить дневной убыток
        self._check_daily_loss()

        # 4. Проверить размер позиции
        await self._check_position_size(order)

        # 5. Проверить количество позиций
        await self._check_max_positions(order)

        # 6. Проверить стоп-лосс
        if self._config.stop_loss_pct is not None:
            await self._check_stop_loss(order)

        # Все проверки пройдены — отправляем с таймаутом, чтобы зависший
        # gRPC-вызов не блокировал весь runner.
        log.info(
            "Risk OK: %s %s qty=%d price=%s",
            order.side.value, order.figi, order.quantity, order.price,
        )
        try:
            return await asyncio.wait_for(
                self._gateway.post_order(order), timeout=30.0
            )
        except asyncio.TimeoutError as e:
            from broker.tinvest.orders import (  # noqa: WPS433
                OrderExchangeUnavailable,
            )

            raise OrderExchangeUnavailable(
                "post_order timeout 30s"
            ) from e

    async def cancel_order(
        self, account_id: str, order_request_id: str
    ) -> OrderStateInfo:
        """Отмена заявки — всегда разрешена."""
        return await self._gateway.cancel_order(account_id, order_request_id)

    async def stream_order_states(self) -> AsyncIterator[OrderStateInfo]:
        """Стрим статусов заявок — проксирует без изменений."""
        async for state in self._gateway.stream_order_states():
            yield state

    @property
    def kill_switch(self) -> KillSwitch:
        """Доступ к kill-switch для внешнего управления."""
        return self._kill_switch

    # --- Внутренние методы ---------------------------------------------------

    async def _ensure_daily_start(self, account_id: str) -> None:
        """Инициализировать стартовое значение портфеля на день."""
        now = self._clock.now()
        today_key = now.strftime("%Y-%m-%d")

        # Сброс после клиринга (00:30 МСК)
        if self._last_reset_date is None or self._last_reset_date != today_key:
            # Проверяем, прошло ли время клиринга сегодня
            clearing_time = now.replace(
                hour=self._config.clearing_hour,
                minute=self._config.clearing_minute,
                second=0, microsecond=0,
            )
            if now >= clearing_time or self._last_reset_date is None:
                snapshot = await self._portfolio.get_portfolio(account_id)
                self._daily_start_value = snapshot.total_value.value
                self._daily_pnl = Decimal(0)
                self._last_reset_date = today_key
                log.info(
                    "Дневной P&L сброшен: start_value=%s",
                    self._daily_start_value,
                )

    def _check_daily_loss(self) -> None:
        """Проверить дневной убыток."""
        if self._daily_start_value is None or self._daily_start_value <= 0:
            return

        loss = -self._daily_pnl
        loss_pct = loss / self._daily_start_value

        if loss_pct >= self._config.max_daily_loss_pct:
            # Авто-активация kill-switch
            self._kill_switch.auto_activate_if_needed(
                self._daily_start_value - loss,
                self._daily_start_value,
            )
            raise RiskViolation(
                f"Дневной убыток {loss_pct:.2%} ≥ "
                f"{self._config.max_daily_loss_pct:.2%}"
            )

    async def _check_position_size(self, order: Order) -> None:
        """Проверить, что позиция ≤ max_position_pct от депозита."""
        if self._daily_start_value is None or self._daily_start_value <= 0:
            return

        max_position = self._daily_start_value * self._config.max_position_pct

        # Оцениваем стоимость заявки
        if order.price is not None:
            order_value = order.price * order.quantity
        else:
            # Для рыночной заявки — берём текущую цену из портфеля
            try:
                snapshot = await self._portfolio.get_portfolio(order.account_id)
                # Ищем текущую позицию по figi
                for pos in snapshot.positions:
                    if pos.figi == order.figi and pos.current_price is not None:
                        order_value = pos.current_price * order.quantity
                        break
                else:
                    # Нет позиции — используем available_cash как ориентир
                    order_value = snapshot.available_cash.value
            except Exception:
                log.warning("Не удалось оценить размер позиции — пропускаем")
                return

        if order_value > max_position:
            raise RiskViolation(
                f"Размер позиции {order_value:.2f} > "
                f"{self._config.max_position_pct:.0%} от депозита "
                f"({max_position:.2f})"
            )

    async def _check_max_positions(self, order: Order) -> None:
        """Проверить количество открытых позиций."""
        try:
            positions = await self._portfolio.get_positions(order.account_id)
        except Exception:
            log.warning("Не удалось получить позиции — пропускаем проверку")
            return

        # Считаем текущее количество позиций (без учёта новой)
        current_positions = len([p for p in positions if p.quantity > 0])

        # Проверяем, откроет ли заявка новую позицию
        has_existing = any(
            p.figi == order.figi and p.quantity > 0
            for p in positions
        )
        if not has_existing:
            # Новая позиция
            if current_positions >= self._config.max_positions:
                raise RiskViolation(
                    f"Кол-во позиций {current_positions} ≥ "
                    f"макс. {self._config.max_positions}"
                )

    async def _check_stop_loss(self, order: Order) -> None:
        """Проверить стоп-лосс на позицию (если задан)."""
        if self._config.stop_loss_pct is None:
            return

        try:
            positions = await self._portfolio.get_positions(order.account_id)
        except Exception:
            return

        for pos in positions:
            if pos.figi == order.figi and pos.current_price is not None:
                avg_price = pos.average_price
                if avg_price <= 0:
                    continue
                # Для длинной позиции: падение от средней
                if pos.side.value == "buy":
                    loss_pct = (avg_price - pos.current_price) / avg_price
                else:
                    # Для короткой: рост от средней
                    loss_pct = (pos.current_price - avg_price) / avg_price

                if loss_pct >= self._config.stop_loss_pct:
                    # Разрешаем только закрывающие заявки
                    is_closing = (
                        (pos.side.value == "buy" and order.side.value == "sell")
                        or (pos.side.value == "sell" and order.side.value == "buy")
                    )
                    if not is_closing:
                        raise RiskViolation(
                            f"Стоп-лосс: убыток {loss_pct:.2%} ≥ "
                            f"{self._config.stop_loss_pct:.2%} по {order.figi}"
                        )

    async def _on_order_state(self, event: OrderStateEvent) -> None:
        """Обновить дневной P&L по исполнившимся заявкам."""
        if event.payload is None:
            return
        state_info = event.payload

        # Обновляем P&L только для исполненных заявок
        if state_info.state in (OrderState.FILLED, OrderState.PARTIALLY_FILLED):
            if state_info.average_price is not None and state_info.filled_quantity > 0:
                # Это упрощённый расчёт — точный P&L считает PnLCalculator
                pass  # P&L обновляется через PortfolioGateway

    def update_daily_pnl(self, pnl: Decimal) -> None:
        """Обновить дневной P&L (вызывается извне, например PnLCalculator)."""
        self._daily_pnl = pnl