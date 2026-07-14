"""Адаптер рыночных данных T-Invest.

Реализует `MarketData` Protocol из `core.interfaces`:
- Подписки на стаканы / свечи / сделки
- Маппинг protobuf-моделей T-Invest → наши pydantic-модели

Важно: методы `subscribe_*` отправляют серверу запрос на подписку, а
`stream_*` читают уже настроенный gRPC-стрим. Подписки + чтение — пара
взаимосвязанных вызовов. Подробности в T-Invest API:
https://tinkoff.github.io/investAPI/marketdata/
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Optional

from tinkoff.invest import (
    CandleInterval,
    InstrumentIdType,
    SubscriptionInterval,
)
from tinkoff.invest.schemas import (
    Candle,
    OrderBook,
    Trade as TinkoffTrade,
)

from core.clock import MOSCOW_TZ, SystemClock
from core.models import (
    Candle as CandleModel,
    Instrument,
    InstrumentKind,
    Order as OrderModel,
    Quote,
    Side,
    Trade as TradeModel,
)
from core.models import Money  # noqa: F401  (зарезервировано для будущих полей)

from .client import TinkoffClient
from .converters import (
    quotation_to_decimal,
    tinkoff_side_to_side,
    tinkoff_time,
)

log = logging.getLogger(__name__)


# Маппинг человекочитаемых интервалов → tinkoff-енумам.
# CandleInterval — для get_candles (исторические свечи): полный набор.
# SubscriptionInterval — для subscribe_candles (стрим): в SDK 0.2.0b59
# поддерживаются только ONE_MINUTE и FIVE_MINUTES, остальное маппится на
# 1m с агрегацией на стороне стратегии.
CANDLE_INTERVAL_MAP = {
    "1m": (CandleInterval.CANDLE_INTERVAL_1_MIN, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE),
    "5m": (CandleInterval.CANDLE_INTERVAL_5_MIN, SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES),
    "15m": (CandleInterval.CANDLE_INTERVAL_15_MIN, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE),
    "1h": (CandleInterval.CANDLE_INTERVAL_HOUR, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE),
    "1d": (CandleInterval.CANDLE_INTERVAL_DAY, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE),
}


# Алиасы для обратной совместимости (используются в тестах)
_tinkoff_side_to_side = tinkoff_side_to_side
_tinkoff_time = tinkoff_time
_quotation_to_decimal = quotation_to_decimal


def _to_orderbook_quote(orderbook) -> Quote:
    """Tinkoff GetOrderBookResponse → core.Quote.

    В SDK 0.2.0b59 GetOrderBookResponse имеет ПЛОСКУЮ структуру:
    figi, bids, asks, depth, last_price, ... лежат на нём самом.
    В старых версиях был вложенный .orderbook — учитываем оба варианта.
    """
    ob = getattr(orderbook, "orderbook", orderbook)
    bids = [
        (_quotation_to_decimal(b.price), int(b.quantity))
        for b in ob.bids
    ]
    asks = [
        (_quotation_to_decimal(a.price), int(a.quantity))
        for a in ob.asks
    ]
    return Quote(
        figi=ob.figi,
        timestamp=_tinkoff_time(getattr(ob, "orderbook_ts", ob.time) if hasattr(ob, "time") else ob.orderbook_ts),
        bids=bids,
        asks=asks,
    )


def _to_trade(trade: TinkoffTrade, account_id: str = "") -> TradeModel:
    """Tinkoff Trade → core.Trade."""
    return TradeModel(
        order_request_id="",  # у биржевой сделки нет нашего request_id
        account_id=account_id,
        figi=trade.figi,
        side=_tinkoff_side_to_side(trade.direction),
        quantity=int(trade.quantity),
        price=_quotation_to_decimal(trade.price),
        timestamp=_tinkoff_time(trade.time),
        trade_id=str(trade.trade_id),
    )


def _to_candle(candle: Candle, figi: Optional[str] = None) -> CandleModel:
    """Tinkoff Candle → core.Candle.

    SDK 0.2.0b59: исторические свечи (HistoricCandle), возвращаемые
    get_candles / stream, не содержат поля .figi. В таком случае берём figi
    из аргумента (передаём известный FIGI подписки).
    """
    candle_figi = getattr(candle, "figi", None) or figi
    if candle_figi is None:
        raise ValueError("Candle без figi: необходимо передать figi явно")
    return CandleModel(
        figi=candle_figi,
        timestamp=_tinkoff_time(candle.time),
        open=_quotation_to_decimal(candle.open),
        high=_quotation_to_decimal(candle.high),
        low=_quotation_to_decimal(candle.low),
        close=_quotation_to_decimal(candle.close),
        volume=int(candle.volume),
    )


class TinkoffMarketData:
    """Реализация MarketData поверх TinkoffClient.

    Использование:
        client = TinkoffClient(cfg)
        with client:
            md = TinkoffMarketData(client)
            await md.subscribe_quotes("FUTRI...")
            async for quote in md.stream_quotes():
                ...
    """

    def __init__(self, client: TinkoffClient) -> None:
        self._client = client
        self._clock = SystemClock()
        self._subscribed_quotes: set[str] = set()
        self._subscribed_trades: set[str] = set()
        self._subscribed_candles: set[tuple[str, str]] = set()

    # --- Подписки (управляют состоянием на сервере) -------------------------

    async def subscribe_quotes(self, figi: str) -> None:
        if figi in self._subscribed_quotes:
            return
        sdk = self._client.sdk
        # MarketDataService.MarketDataStream + subscribe_orderbook
        # Используем именно запрос последнего стакана через
        # get_order_book, чтобы синхронизироваться, + стрим для обновлений.
        # В песочнице проще polling. Стрим оставляем для прод-режима.
        self._subscribed_quotes.add(figi)
        log.info("Subscribed quotes: %s", figi)

    async def subscribe_trades(self, figi: str) -> None:
        if figi in self._subscribed_trades:
            return
        sdk = self._client.sdk
        # В SDK-клиенте tinkoff.invest подписки на стримы инкапсулированы
        # в MarketDataStream. Здесь фиксируем намерение — реальный стрим
        # открывается в stream_trades().
        self._subscribed_trades.add(figi)
        log.info("Subscribed trades: %s", figi)

    async def subscribe_candles(self, figi: str, interval: str) -> None:
        key = (figi, interval)
        if key in self._subscribed_candles:
            return
        if interval not in CANDLE_INTERVAL_MAP:
            raise ValueError(
                f"Неподдерживаемый интервал '{interval}'. "
                f"Допустимо: {list(CANDLE_INTERVAL_MAP)}"
            )
        self._subscribed_candles.add(key)
        log.info("Subscribed candles: %s %s", figi, interval)

    # --- Стримы ------------------------------------------------------------

    async def stream_quotes(self) -> AsyncIterator[Quote]:
        """Стрим обновлений стаканов по подписанным figi.

        В текущей версии используется **polling через GetOrderBook** —
        SDK в синхронном режиме не предоставляет готовой обёртки над
        server-streaming `subscribe_order_book`, а async-обёртки
        в tinkoff-investments 0.2.0b59 требуют доработки. Polling раз
        в 500 мс — приемлемо для интрадей-стратегий на 1-5 мин.
        """
        sdk = self._client.sdk
        while True:
            for figi in list(self._subscribed_quotes):
                try:
                    ob = sdk.market_data.get_order_book(
                        figi=figi, depth=20
                    )
                    yield _to_orderbook_quote(ob)
                except Exception as e:  # noqa: BLE001
                    log.warning("get_order_book(%s) failed: %s", figi, e)
            await asyncio.sleep(0.5)

    async def stream_trades(self) -> AsyncIterator[TradeModel]:
        """Стрим сделок по подписанным figi.

        Polling через `get_last_trades` (последние N сделок). При
        необходимости точной реалтайм-доставки заменить на
        async-обёртку над `trades_stream`.
        """
        sdk = self._client.sdk
        seen: set[str] = set()
        while True:
            for figi in list(self._subscribed_trades):
                try:
                    resp = sdk.market_data.get_last_trades(figi=figi)
                    for t in resp.trades:
                        tid = str(t.trade_id)
                        if tid in seen:
                            continue
                        seen.add(tid)
                        yield _to_trade(t)
                except Exception as e:  # noqa: BLE001
                    log.warning("get_last_trades(%s) failed: %s", figi, e)
            await asyncio.sleep(0.5)

    async def stream_candles(self) -> AsyncIterator[CandleModel]:
        """Стрим свечей по подписанным (figi, interval).

        Polling `get_candles` за последние 2 свечи + эмитим только
        те, что появились с прошлой итерации.
        """
        sdk = self._client.sdk
        seen: set[tuple[str, datetime]] = set()
        while True:
            for figi, interval in list(self._subscribed_candles):
                ci, _ = CANDLE_INTERVAL_MAP[interval]
                try:
                    # Берём последние 2 свечи, фильтруем уже виденные
                    resp = sdk.market_data.get_candles(
                        figi=figi,
                        from_=_now_minus_minutes(5),  # см. helper ниже
                        to=datetime.now(MOSCOW_TZ),
                        interval=ci,
                    )
                    for c in resp.candles:
                        ts = _tinkoff_time(c.time)
                        key = (figi, ts)
                        if key in seen:
                            continue
                        seen.add(key)
                        yield _to_candle(c, figi=figi)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "get_candles(%s, %s) failed: %s", figi, interval, e
                    )
            await asyncio.sleep(1.0)

    # --- Исторические данные ------------------------------------------------

    async def get_last_candles(
        self, figi: str, interval: str, count: int
    ) -> list[CandleModel]:
        if interval not in CANDLE_INTERVAL_MAP:
            raise ValueError(
                f"Неподдерживаемый интервал '{interval}'. "
                f"Допустимо: {list(CANDLE_INTERVAL_MAP)}"
            )
        ci, _ = CANDLE_INTERVAL_MAP[interval]
        sdk = self._client.sdk
        # Берём с запасом по времени — T-Invest требует from_/to, не count
        resp = sdk.market_data.get_candles(
            figi=figi,
            from_=_now_minus_minutes(_interval_to_minutes(interval) * count),
            to=datetime.now(MOSCOW_TZ),
            interval=ci,
        )
        candles = [_to_candle(c, figi=figi) for c in resp.candles]
        return candles[-count:]

    # --- Инструменты --------------------------------------------------------

    async def get_instrument(self, figi: str) -> Instrument:
        sdk = self._client.sdk
        resp = sdk.instruments.get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=figi
        )
        ins = resp.instrument
        return _to_instrument(ins)

    async def find_instruments(
        self, ticker: Optional[str] = None, kind: Optional[str] = None
    ) -> list[Instrument]:
        sdk = self._client.sdk
        out: list[Instrument] = []
        # Ищем только фьючерсы (по CLAUDE.md — class_code=SPBFUT)
        resp = sdk.instruments.futures()
        for f in resp.instruments:
            if ticker and ticker.upper() not in f.ticker.upper():
                continue
            if kind and str(f.instrument_kind).lower() != kind.lower():
                continue
            out.append(_to_instrument(f))
        return out


# --- Вспомогательные ---------------------------------------------------------


def _to_instrument(f) -> Instrument:
    """Tinkoff Future (или любой Instrument) → core.Instrument."""
    return Instrument(
        figi=f.figi,
        ticker=f.ticker,
        class_code=getattr(f, "class_code", "SPBFUT"),
        name=f.name,
        lot=int(getattr(f, "lot", 1)),
        kind=InstrumentKind.FUTURES,
        min_step=Decimal(getattr(f, "min_step", 1) or 1),
        min_price_increment=Decimal(
            str(getattr(f, "min_price_increment", 0) or 0)
        ),
    )


def _interval_to_minutes(interval: str) -> int:
    return {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}.get(interval, 1)


def _now_minus_minutes(minutes: int):
    """datetime в МСК за N минут до сейчас (используется в from_)."""
    from datetime import timedelta
    return datetime.now(MOSCOW_TZ) - timedelta(minutes=minutes)
