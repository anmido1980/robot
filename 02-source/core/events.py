"""Простая шина событий (pub/sub).

Используется для передачи данных между слоями: broker → strategies → risk → journal.
Слои не знают друг о друге — только подписываются на типы событий, которые им интересны.

Для интрадей-робота хватит синхронной in-process шины. Если позже понадобится
масштабирование — заменяется на Redis/ZeroMQ без изменения подписчиков.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

from .models import Candle, OrderStateInfo, Position, Quote, Signal, Trade


@dataclass(frozen=True)
class Event:
    """Базовый класс события. Поле type нужно для дискриминации в шине."""

    type: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class QuoteEvent(Event):
    payload: Optional[Quote] = None

    def __init__(self, payload: Quote) -> None:  # type: ignore[override]
        super().__init__(type="quote", timestamp=payload.timestamp)
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True)
class TradeEvent(Event):
    payload: Optional[Trade] = None

    def __init__(self, payload: Trade) -> None:  # type: ignore[override]
        super().__init__(type="trade", timestamp=payload.timestamp)
        object.__setattr__(self, "payload", payload)


def _trade_to_trade_event(trade: Trade) -> TradeEvent:
    """Утилита для публикации сделки как TradeEvent (помимо OrderStateEvent)."""
    return TradeEvent(trade)


@dataclass(frozen=True)
class CandleEvent(Event):
    payload: Optional[Candle] = None

    def __init__(self, payload: Candle) -> None:  # type: ignore[override]
        super().__init__(type="candle", timestamp=payload.timestamp)
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True)
class OrderStateEvent(Event):
    payload: Optional[OrderStateInfo] = None

    def __init__(self, payload: OrderStateInfo) -> None:  # type: ignore[override]
        super().__init__(type="order_state", timestamp=payload.timestamp)
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True)
class PositionEvent(Event):
    payload: Optional[Position] = None

    def __init__(self, payload: Position) -> None:  # type: ignore[override]
        super().__init__(type="position")
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True)
class SignalEvent(Event):
    payload: Optional[Signal] = None

    def __init__(self, payload: Signal) -> None:  # type: ignore[override]
        super().__init__(type="signal", timestamp=payload.timestamp)
        object.__setattr__(self, "payload", payload)


Subscriber = Callable[[Event], Awaitable[None]]


class EventBus:
    """Асинхронная шина событий. Подписчики получают события по типу.

    Пример:
        bus = EventBus()
        async def on_quote(event: QuoteEvent):
            ...
        bus.subscribe("quote", on_quote)
        await bus.publish(QuoteEvent(quote))
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Subscriber]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: str, handler: Subscriber) -> None:
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Subscriber) -> None:
        if handler in self._subscribers[event_type]:
            self._subscribers[event_type].remove(handler)

    async def publish(self, event: Event) -> None:
        handlers = list(self._subscribers.get(event.type, []))
        if not handlers:
            return
        # Fan-out: все подписчики получают событие; ошибка в одном не блокирует других
        await asyncio.gather(
            *(h(event) for h in handlers), return_exceptions=True
        )


__all__ = [
    "Event",
    "EventBus",
    "QuoteEvent",
    "TradeEvent",
    "CandleEvent",
    "OrderStateEvent",
    "PositionEvent",
    "SignalEvent",
    "Subscriber",
]
