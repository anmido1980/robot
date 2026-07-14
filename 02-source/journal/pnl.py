"""Калькулятор P&L: реализованный, нереализованный, equity curve.

Отслеживает позиции по сделкам и рыночным ценам.
Сброс реализованного P&L — при клиринге (00:30 МСК).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from core.clock import Clock, MOSCOW_TZ
from core.models import Side, Trade

log = logging.getLogger(__name__)


@dataclass
class PositionTracker:
    """Отслеживание средней цены и P&L по одному инструменту."""

    figi: str
    side: Side = Side.BUY
    quantity: int = 0
    average_price: Decimal = Decimal(0)
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    current_price: Optional[Decimal] = None

    def update_fill(self, trade: Trade) -> None:
        """Обновить позицию по сделке.

        Для покупок: увеличиваем позицию, пересчитываем среднюю цену.
        Для продаж: уменьшаем позицию, фиксируем реализованный P&L.
        """
        if trade.side == Side.BUY:
            # Покупка — увеличиваем позицию
            total_cost = self.average_price * self.quantity + trade.price * trade.quantity
            self.quantity += trade.quantity
            if self.quantity > 0:
                self.average_price = total_cost / self.quantity
            self.side = Side.BUY
        else:
            # Продажа — фиксируем P&L
            if self.quantity > 0:
                pnl_per_unit = trade.price - self.average_price
                closed_qty = min(trade.quantity, self.quantity)
                self.realized_pnl += pnl_per_unit * closed_qty
                self.quantity -= closed_qty
                if self.quantity == 0:
                    self.average_price = Decimal(0)
                    self.side = Side.BUY  # сброс

    def update_market_price(self, price: Decimal) -> None:
        """Обновить рыночную цену и нереализованный P&L."""
        self.current_price = price
        if self.quantity > 0 and self.average_price > 0:
            if self.side == Side.BUY:
                self.unrealized_pnl = (price - self.average_price) * self.quantity
            else:
                self.unrealized_pnl = (self.average_price - price) * self.quantity
        else:
            self.unrealized_pnl = Decimal(0)


@dataclass
class EquityPoint:
    """Точка на кривой эквити."""

    timestamp: datetime
    value: Decimal


class PnLCalculator:
    """Расчёт P&L по сделкам и рыночным ценам.

    Использование:
        pnl = PnLCalculator(clock)
        pnl.update_fill(trade)
        pnl.update_market_price(figi, price)
        print(pnl.realized_pnl(), pnl.unrealized_pnl())
    """

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._positions: dict[str, PositionTracker] = {}
        self._equity_curve: list[EquityPoint] = []
        self._daily_realized_pnl: Decimal = Decimal(0)
        self._last_reset_date: Optional[str] = None

    # --- Обновление данных ----------------------------------------------------

    def update_fill(self, trade: Trade) -> None:
        """Обновить позицию по сделке."""
        if trade.figi not in self._positions:
            self._positions[trade.figi] = PositionTracker(figi=trade.figi)

        tracker = self._positions[trade.figi]
        prev_realized = tracker.realized_pnl
        tracker.update_fill(trade)
        # Добавляем разницу реализованного P&L к дневному
        self._daily_realized_pnl += tracker.realized_pnl - prev_realized

    def update_market_price(self, figi: str, price: Decimal) -> None:
        """Обновить рыночную цену инструмента."""
        if figi not in self._positions:
            return
        self._positions[figi].update_market_price(price)

    def record_equity_point(self, total_value: Decimal) -> None:
        """Записать точку на кривой эквити."""
        self._equity_curve.append(
            EquityPoint(timestamp=self._clock.now(), value=total_value)
        )

    # --- Чтение ---------------------------------------------------------------

    def realized_pnl(self) -> Decimal:
        """Суммарный реализованный P&L по всем позициям."""
        return sum(p.realized_pnl for p in self._positions.values())

    def unrealized_pnl(self) -> Decimal:
        """Суммарный нереализованный P&L по всем позициям."""
        return sum(p.unrealized_pnl for p in self._positions.values())

    def total_pnl(self) -> Decimal:
        """Общий P&L (реализованный + нереализованный)."""
        return self.realized_pnl() + self.unrealized_pnl()

    def daily_realized_pnl(self) -> Decimal:
        """Дневной реализованный P&L (сбрасывается при клиринге)."""
        return self._daily_realized_pnl

    def equity_curve(self) -> list[EquityPoint]:
        """Кривая эквити."""
        return list(self._equity_curve)

    def get_position(self, figi: str) -> Optional[PositionTracker]:
        """Получить трекер позиции по FIGI."""
        return self._positions.get(figi)

    def all_positions(self) -> list[PositionTracker]:
        """Все открытые позиции."""
        return [p for p in self._positions.values() if p.quantity > 0]

    # --- Сброс при клиринге ---------------------------------------------------

    def check_and_reset_daily(self, clearing_hour: int = 0, clearing_minute: int = 30) -> None:
        """Сбросить дневной P&L, если наступило время клиринга.

        Вызывать периодически (например, в runner).
        """
        now = self._clock.now()
        today_key = now.strftime("%Y-%m-%d")

        if self._last_reset_date == today_key:
            return

        clearing_time = now.replace(
            hour=clearing_hour, minute=clearing_minute,
            second=0, microsecond=0,
        )
        if now >= clearing_time:
            self._daily_realized_pnl = Decimal(0)
            self._last_reset_date = today_key
            log.info("Дневной P&L сброшен (клиринг 0%d:%02d МСК)",
                     clearing_hour, clearing_minute)

    # --- Сериализация / восстановление ----------------------------------------

    def to_state(self) -> dict[str, Any]:
        """Сериализовать состояние P&L."""
        return {
            "daily_realized_pnl": str(self._daily_realized_pnl),
            "last_reset_date": self._last_reset_date,
            "positions": [
                {
                    "figi": pos.figi,
                    "side": pos.side.value,
                    "quantity": pos.quantity,
                    "average_price": str(pos.average_price),
                    "realized_pnl": str(pos.realized_pnl),
                    "current_price": str(pos.current_price) if pos.current_price is not None else None,
                }
                for pos in self._positions.values()
            ],
            "equity_curve": [
                {"timestamp": p.timestamp.isoformat(), "value": str(p.value)}
                for p in self._equity_curve
            ],
        }

    def from_state(self, state: dict[str, Any]) -> None:
        """Восстановить состояние P&L из словаря."""
        self._daily_realized_pnl = Decimal(state.get("daily_realized_pnl", "0"))
        self._last_reset_date = state.get("last_reset_date")

        self._positions = {}
        for pos_data in state.get("positions", []):
            pos = PositionTracker(
                figi=pos_data["figi"],
                side=Side(pos_data["side"]),
                quantity=int(pos_data["quantity"]),
                average_price=Decimal(pos_data.get("average_price", "0")),
                realized_pnl=Decimal(pos_data.get("realized_pnl", "0")),
            )
            current_price_raw = pos_data.get("current_price")
            if current_price_raw is not None:
                pos.current_price = Decimal(current_price_raw)
                pos.update_market_price(pos.current_price)
            self._positions[pos.figi] = pos

        self._equity_curve = [
            EquityPoint(
                timestamp=datetime.fromisoformat(p["timestamp"]),
                value=Decimal(p["value"]),
            )
            for p in state.get("equity_curve", [])
        ]

    def replay_trades(self, trades: list[Trade], since: Optional[datetime] = None) -> None:
        """Воспроизвести список сделок для восстановления позиций и P&L.

        Если задан since — пропускаются сделки раньше since (например, чтобы
        не пересчитывать вчерашние при восстановлении после клиринга).
        """
        filtered = trades
        if since is not None:
            filtered = [t for t in trades if t.timestamp >= since]
        for trade in filtered:
            self.update_fill(trade)
        log.info("Replayed %d trades into PnLCalculator", len(filtered))