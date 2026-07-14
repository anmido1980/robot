"""Адаптер портфеля T-Invest.

Реализует `PortfolioGateway` Protocol из `core.interfaces`:
- get_portfolio — снимок портфеля (деньги + позиции)
- get_positions — список позиций (фьючерсы с балансами)
- get_instrument — метаданные инструмента по FIGI
- find_instruments — поиск инструментов (по ticker, типу)
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from tinkoff.invest import InstrumentIdType
from tinkoff.invest.exceptions import RequestError

from core.clock import SystemClock
from core.models import (
    Instrument,
    InstrumentKind,
    Money,
    Position,
    PortfolioSnapshot,
    Side,
)

from .client import TinkoffClient
from .converters import (
    quotation_to_decimal,
    tinkoff_time,
)

log = logging.getLogger(__name__)


# --- Маппинги ---------------------------------------------------------------

# instrument_type (строка из SDK) → InstrumentKind
_INSTRUMENT_TYPE_MAP = {
    "futures": InstrumentKind.FUTURES,
    "future": InstrumentKind.FUTURES,
    "option": InstrumentKind.OPTION,
    "options": InstrumentKind.OPTION,
}


def _money_to_model(mv) -> Money:
    """MoneyValue (SDK) → core.Money."""
    if mv is None:
        return Money(value=Decimal(0), currency="rub")
    return Money(
        value=quotation_to_decimal(mv),
        currency=getattr(mv, "currency", "rub") or "rub",
    )


def _position_side(quantity: Decimal) -> Side:
    """Определить сторону позиции по количеству."""
    if quantity > 0:
        return Side.BUY  # long
    if quantity < 0:
        return Side.SELL  # short
    return Side.BUY  # нулевая позиция — формально long


def _portfolio_position_to_position(
    pos, account_id: str
) -> Position:
    """PortfolioPosition (SDK) → core.Position.

    Используется в get_portfolio. PortfolioPosition.quantity — Quotation,
    может быть дробным (для акций). Для фьючерсов — целое число лотов.
    """
    quantity = quotation_to_decimal(pos.quantity)
    avg_price = (
        _money_to_model(pos.average_position_price)
        if pos.average_position_price
        else Money(value=Decimal(0), currency="rub")
    )
    current_price = (
        _money_to_model(pos.current_price)
        if pos.current_price
        else None
    )
    unrealized_pnl = (
        _money_to_model(pos.expected_yield)
        if pos.expected_yield
        else None
    )
    # Определяем тип инструмента из instrument_type
    instr_type = getattr(pos, "instrument_type", "").lower()
    kind = _INSTRUMENT_TYPE_MAP.get(instr_type, InstrumentKind.FUTURES)

    return Position(
        account_id=account_id,
        figi=pos.figi,
        ticker=getattr(pos, "ticker", ""),
        side=_position_side(quantity),
        quantity=abs(int(quantity)),  # абсолютное число лотов
        average_price=avg_price.value,
        current_price=current_price.value if current_price else None,
        unrealized_pnl=unrealized_pnl,
    )


def _futures_position_to_position(
    fut_pos, account_id: str, ticker_map: dict[str, str]
) -> Position:
    """PositionsFutures (SDK) → core.Position.

    Используется в get_positions. PositionsFutures.balance — int,
    отрицательный для шорта. Нет данных о средней цене.
    """
    figi = fut_pos.figi
    balance = int(fut_pos.balance)  # может быть отрицательным
    ticker = ticker_map.get(figi, figi)  # fallback на figi если ticker неизвестен

    return Position(
        account_id=account_id,
        figi=figi,
        ticker=ticker,
        side=_position_side(Decimal(balance)),
        quantity=abs(balance),
        average_price=Decimal(0),  # нет в PositionsFutures
        current_price=None,
        unrealized_pnl=None,
    )


def _future_to_instrument(f) -> Instrument:
    """Future (SDK) → core.Instrument."""
    min_step = quotation_to_decimal(f.min_price_increment) if f.min_price_increment else Decimal(1)
    return Instrument(
        figi=f.figi,
        ticker=f.ticker,
        class_code=getattr(f, "class_code", "SPBFUT"),
        name=f.name,
        lot=int(getattr(f, "lot", 1)),
        kind=InstrumentKind.FUTURES,
        min_step=min_step,
        min_price_increment=min_step,  # для фьючерсов = min_step
    )


# --- Основной класс ----------------------------------------------------------


class TinkoffPortfolio:
    """Реализация PortfolioGateway поверх TinkoffClient.

    Использование:
        client = TinkoffClient(cfg)
        with client:
            pf = TinkoffPortfolio(client)
            snapshot = await pf.get_portfolio("account-id")
    """

    def __init__(self, client: TinkoffClient) -> None:
        self._client = client
        self._clock = SystemClock()
        # Кэш тикеров: figi → ticker, заполняется при поиске инструментов
        self._ticker_map: dict[str, str] = {}

    # --- Публичный API --------------------------------------------------------

    async def get_portfolio(self, account_id: str) -> PortfolioSnapshot:
        """Снимок портфеля (деньги + позиции).

        Использует OperationsService.get_portfolio() — возвращает
        PortfolioResponse с total_amount_portfolio и positions.
        """
        sdk = self._client.sdk
        try:
            resp = sdk.operations.get_portfolio(account_id=account_id)
        except RequestError as exc:
            raise PortfolioError(
                f"Ошибка получения портфеля: {exc.details}"
            ) from exc

        # Общая стоимость портфеля
        total_value = _money_to_model(
            getattr(resp, "total_amount_portfolio", None)
        )
        # Доступные средства — total_amount_currencies (для фьючерсов
        # это ГО + свободные средства)
        available_cash = _money_to_model(
            getattr(resp, "total_amount_currencies", None)
        )

        # Маппинг позиций
        positions = [
            _portfolio_position_to_position(p, account_id)
            for p in resp.positions
            if p.figi  # пропускаем пустые
        ]

        # Обновляем кэш тикеров из позиций
        for p in resp.positions:
            if p.figi and getattr(p, "ticker", None):
                self._ticker_map[p.figi] = p.ticker

        return PortfolioSnapshot(
            account_id=account_id,
            timestamp=self._clock.now(),
            total_value=total_value,
            available_cash=available_cash,
            positions=positions,
        )

    async def get_positions(self, account_id: str) -> list[Position]:
        """Список позиций (фьючерсы с балансами).

        Использует OperationsService.get_positions() — возвращает
        PositionsResponse с futures[] и money[].
        Более детальная информация по фьючерсам, чем PortfolioResponse.
        """
        sdk = self._client.sdk
        try:
            resp = sdk.operations.get_positions(account_id=account_id)
        except RequestError as exc:
            raise PortfolioError(
                f"Ошибка получения позиций: {exc.details}"
            ) from exc

        positions = [
            _futures_position_to_position(fp, account_id, self._ticker_map)
            for fp in resp.futures
            if fp.figi  # пропускаем пустые
        ]

        # Также включаем ценные бумаги (акции, облигации)
        for sec in resp.securities:
            if sec.figi:
                balance = int(sec.balance)
                if balance == 0:
                    continue
                ticker = self._ticker_map.get(sec.figi, sec.figi)
                positions.append(
                    Position(
                        account_id=account_id,
                        figi=sec.figi,
                        ticker=ticker,
                        side=_position_side(Decimal(balance)),
                        quantity=abs(balance),
                        average_price=Decimal(0),
                        current_price=None,
                        unrealized_pnl=None,
                    )
                )

        return positions

    async def get_instrument(self, figi: str) -> Instrument:
        """Метаданные инструмента по FIGI.

        Сначала пробуем futures(), затем share_by() / bond_by() / currency_by().
        Фоллбэк — get_instrument_by() (универсальный метод).
        """
        sdk = self._client.sdk

        # Пробуем фьючерсы — основной инструмент в scope проекта
        try:
            futures_resp = sdk.instruments.futures()
            for f in futures_resp.instruments:
                if f.figi == figi:
                    inst = _future_to_instrument(f)
                    self._ticker_map[figi] = inst.ticker
                    return inst
        except RequestError:
            pass  # игнорируем — пробуем дальше

        # Универсальный метод
        try:
            resp = sdk.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                id=figi,
            )
            ins = resp.instrument
            return _sdk_instrument_to_model(ins)
        except RequestError as exc:
            raise PortfolioError(
                f"Инструмент не найден: figi={figi}: {exc.details}"
            ) from exc

    async def find_instruments(
        self,
        ticker: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> list[Instrument]:
        """Поиск инструментов.

        По умолчанию ищет фьючерсы (class_code=SPBFUT).
        kind может быть 'futures' или 'option'.
        """
        sdk = self._client.sdk
        result: list[Instrument] = []

        # Фьючерсы — основной инструмент
        if kind is None or kind.lower() in ("futures",):
            try:
                futures_resp = sdk.instruments.futures()
                for f in futures_resp.instruments:
                    if ticker and ticker.upper() not in f.ticker.upper():
                        continue
                    inst = _future_to_instrument(f)
                    self._ticker_map[inst.figi] = inst.ticker
                    result.append(inst)
            except RequestError as exc:
                log.warning("Ошибка поиска фьючерсов: %s", exc)

        # Опционы — зарезервировано (фаза 5+)
        if kind and kind.lower() in ("option",):
            log.info("Поиск опционов пока не реализован")

        return result


# --- Исключения --------------------------------------------------------------


class PortfolioError(Exception):
    """Ошибка при работе с портфелем."""


# --- Вспомогательные ---------------------------------------------------------


def _sdk_instrument_to_model(ins) -> Instrument:
    """Универсальный Instrument (SDK) → core.Instrument.

    Используется для get_instrument_by(), который возвращает
    общий тип Instrument без специфичных полей фьючерсов.
    """
    instr_type = getattr(ins, "instrument_type", "").lower()
    kind = _INSTRUMENT_TYPE_MAP.get(instr_type, InstrumentKind.FUTURES)

    min_step = Decimal(1)
    if hasattr(ins, "min_price_increment") and ins.min_price_increment is not None:
        min_step = quotation_to_decimal(ins.min_price_increment)

    min_price_increment = min_step
    if hasattr(ins, "min_price_increment") and ins.min_price_increment is not None:
        min_price_increment = quotation_to_decimal(ins.min_price_increment)

    return Instrument(
        figi=ins.figi,
        ticker=getattr(ins, "ticker", ""),
        class_code=getattr(ins, "class_code", ""),
        name=getattr(ins, "name", ""),
        lot=int(getattr(ins, "lot", 1)),
        kind=kind,
        min_step=min_step,
        min_price_increment=min_price_increment,
    )


__all__ = ["TinkoffPortfolio", "PortfolioError"]