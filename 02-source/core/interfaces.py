"""Интерфейсы (Protocol) для внешних зависимостей.

Стратегии, риск-менеджер и runner работают только с этими интерфейсами.
Реализации — в `broker/tinvest/` (боевой) и в `tests/mocks/` (тестовый).
"""

from __future__ import annotations

from typing import AsyncIterator, Optional, Protocol, runtime_checkable

from .models import (
    BacktestResult,
    Candle,
    Instrument,
    Order,
    OrderStateInfo,
    PortfolioSnapshot,
    Position,
    Quote,
    Signal,
    Trade,
)


@runtime_checkable
class MarketData(Protocol):
    """Подписки на рыночные данные."""

    async def subscribe_quotes(self, figi: str) -> None:
        """Подписаться на стаканы по инструменту."""
        ...

    async def subscribe_trades(self, figi: str) -> None:
        """Подписаться на ленту сделок по инструменту."""
        ...

    async def subscribe_candles(
        self, figi: str, interval: str
    ) -> None:
        """Подписаться на свечи. interval: '1m' / '5m' / '1h' / '1d'."""
        ...

    async def stream_quotes(self) -> AsyncIterator[Quote]:
        """Асинхронный стрим стаканов."""
        ...

    async def stream_trades(self) -> AsyncIterator[Trade]:
        """Асинхронный стрим сделок."""
        ...

    async def stream_candles(self) -> AsyncIterator[Candle]:
        """Асинхронный стрим свечей."""
        ...

    async def get_last_candles(
        self, figi: str, interval: str, count: int
    ) -> list[Candle]:
        """Исторические свечи (для бэктеста и инициализации индикаторов)."""
        ...


@runtime_checkable
class OrderGateway(Protocol):
    """Выставление, отмена и наблюдение за заявками."""

    async def post_order(self, order: Order) -> OrderStateInfo:
        """Отправить заявку. Идемпотентность через order.order_request_id.

        Raises:
            OrderRejected: биржа/брокер отклонил заявку
            RiskViolation: заявка не прошла риск-менеджер
            ConnectionError: проблема с gRPC-каналом
        """
        ...

    async def cancel_order(
        self, account_id: str, order_request_id: str
    ) -> OrderStateInfo:
        """Снять активную заявку."""
        ...

    async def stream_order_states(self) -> AsyncIterator[OrderStateInfo]:
        """Стрим изменений статусов заявок (OrderStateStream)."""
        ...


@runtime_checkable
class PortfolioGateway(Protocol):
    """Запросы портфеля и позиций."""

    async def get_portfolio(self, account_id: str) -> PortfolioSnapshot:
        """Снимок портфеля (деньги + позиции)."""
        ...

    async def get_positions(self, account_id: str) -> list[Position]:
        """Только позиции."""
        ...

    async def get_instrument(self, figi: str) -> Instrument:
        """Метаданные инструмента по FIGI."""
        ...

    async def find_instruments(
        self, ticker: Optional[str] = None, kind: Optional[str] = None
    ) -> list[Instrument]:
        """Поиск инструментов (например, все фьючерсы с ticker=RI*)."""
        ...


@runtime_checkable
class Clock(Protocol):
    """Единый источник времени. Сейчас — обёртка над datetime; в бою можно
    получать серверное время из ответов T-Invest (поля time в стримах)."""

    def now(self):
        """Текущее время. Возвращает datetime с tzinfo=UTC+3."""
        ...


@runtime_checkable
class Strategy(Protocol):
    """Интерфейс торговой стратегии.

    Получает свечи, выдаёт сигналы. Не знает о брокере, рисках или заказах.
    """

    strategy_id: str

    def on_candle(self, candle: Candle) -> Optional[Signal]:
        """Обработать новую свечу. Вернуть Signal или None."""
        ...

    def on_fill(self, trade: Trade) -> None:
        """Уведомление об исполнении сделки (для обновления внутреннего состояния)."""
        ...

    def reset(self) -> None:
        """Сбросить внутреннее состояние (для walk-forward: новый участок данных)."""
        ...
