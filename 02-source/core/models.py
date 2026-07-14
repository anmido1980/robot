"""Pydantic-модели доменной области.

Все слои выше `core` (risk, strategies, runner) оперируют ТОЛЬКО этими
моделями. Никаких dict'ов, dataclass'ов или брокерских protobuf-структур
в бизнес-логике быть не должно.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# --- Перечисления -----------------------------------------------------------


class Side(str, Enum):
    """Направление заявки/позиции."""

    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Тип заявки."""

    LIMIT = "limit"
    MARKET = "market"
    BESTPRICE = "bestprice"  # «по лучшей цене»


class OrderState(str, Enum):
    """Жизненный цикл заявки (по T-Invest: ExecutionReportStatus)."""

    NEW = "new"
    PENDING = "pending"  # принята биржей, ожидает исполнения
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(str, Enum):
    """Срок действия заявки. В интрадее — всегда DAY."""

    DAY = "day"
    GTC = "gtc"  # until cancelled
    IOC = "ioc"  # immediate or cancel
    FOK = "fok"  # fill or kill


class InstrumentKind(str, Enum):
    """Тип инструмента. В scope проекта — только FUTURES (срочный рынок)."""

    FUTURES = "futures"
    OPTION = "option"  # зарезервировано на фазу опционных конструкций


# --- Деньги -----------------------------------------------------------------


class Money(BaseModel):
    """Денежная сумма. Decimal, чтобы не терять копейки на float."""

    model_config = ConfigDict(frozen=True)

    value: Decimal
    currency: str = Field(default="rub", min_length=3, max_length=3)


# --- Инструмент -------------------------------------------------------------


class Instrument(BaseModel):
    """Метаданные торгуемого инструмента (фьючерс)."""

    model_config = ConfigDict(frozen=True)

    figi: str
    ticker: str
    class_code: str  # SPBFUT
    name: str
    lot: int = Field(ge=1)
    kind: InstrumentKind = InstrumentKind.FUTURES
    min_step: Decimal  # минимальный шаг цены
    min_price_increment: Decimal  # стоимость минимального шага


# --- Заявка -----------------------------------------------------------------


class Order(BaseModel):
    """Заявка, отправляемая в брокер."""

    model_config = ConfigDict(frozen=False)  # допускаем обновление статуса

    # Идентификация
    order_request_id: str  # UUID4 — идемпотентность в T-Invest
    account_id: str
    figi: str

    # Содержание
    side: Side
    order_type: OrderType
    quantity: int = Field(ge=1)
    price: Optional[Decimal] = None  # для лимитки; для рыночной = None
    time_in_force: TimeInForce = TimeInForce.DAY

    # Контекст (проставляется стратегией/риском)
    strategy_id: Optional[str] = None  # какой стратегии принадлежит
    comment: Optional[str] = None  # для журнала сделок


class OrderStateInfo(BaseModel):
    """Текущий статус заявки. Приходит из OrderStateStream."""

    order_request_id: str
    account_id: str
    state: OrderState
    filled_quantity: int = 0
    average_price: Optional[Decimal] = None

    # Сообщение об ошибке (если state == REJECTED)
    reject_reason: Optional[str] = None

    # Инструмент и направление (из SDK OrderState / operation или runner comment)
    figi: Optional[str] = None
    side: Optional[Side] = None

    # Дополнительный контекст от runner'а (figi/side)
    comment: Optional[str] = None

    # Серверное время события (UTC+3)
    timestamp: datetime


class Trade(BaseModel):
    """Исполнение (сделка) по заявке."""

    order_request_id: str
    account_id: str
    figi: str
    side: Side
    quantity: int
    price: Decimal
    timestamp: datetime
    trade_id: str  # биржевой ID сделки


# --- Позиция и портфель -----------------------------------------------------


class Position(BaseModel):
    """Открытая позиция по одному инструменту."""

    account_id: str
    figi: str
    ticker: str
    side: Side  # BUY = long, SELL = short
    quantity: int  # абсолютное число лотов
    average_price: Decimal
    current_price: Optional[Decimal] = None  # обновляется по рыночным данным
    unrealized_pnl: Optional[Money] = None


class PortfolioSnapshot(BaseModel):
    """Снимок портфеля на момент времени."""

    account_id: str
    timestamp: datetime
    total_value: Money
    available_cash: Money
    positions: list[Position] = Field(default_factory=list)


# --- Рыночные данные --------------------------------------------------------


class Candle(BaseModel):
    """Свеча OHLCV."""

    figi: str
    timestamp: datetime  # начало свечи
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int  # в лотах


# --- Сигнал стратегии -------------------------------------------------------


class Signal(BaseModel):
    """Сигнал на вход/выход из стратегии."""

    model_config = ConfigDict(frozen=True)

    figi: str
    side: Side  # BUY = вход в long, SELL = вход в short / закрытие long
    price: Optional[Decimal] = None  # желаемая цена входа (None = market)
    strategy_id: str
    reason: str  # текстовое описание причины сигнала (для журнала)
    timestamp: datetime  # время генерации сигнала


# --- Результат бэктеста -----------------------------------------------------


class Regime(str, Enum):
    """Режим рынка. Используется RegimeClassifier и ComboStrategy."""

    TREND = "trend"
    RANGE = "range"
    BREAKOUT = "breakout"


class RegimeScores(BaseModel):
    """Скоры режимов рынка на одну свечу.

    Каждый скор в диапазоне [0, 1]. Чем выше, тем больше текущий
    бар похож на соответствующий режим. Сумма скоров не обязана быть 1.

    Используется ComboStrategy для выбора базовой стратегии.
    """

    model_config = ConfigDict(frozen=True)

    trend: float = Field(ge=0.0, le=1.0)
    range: float = Field(ge=0.0, le=1.0)
    breakout: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)  # max(trend, range, breakout)
    flat: bool  # True, если confidence < порога

    @property
    def top_regime(self) -> Regime:
        """Режим с наивысшим скором (или TREND по умолчанию)."""
        scores = {
            Regime.TREND: self.trend,
            Regime.RANGE: self.range,
            Regime.BREAKOUT: self.breakout,
        }
        return max(scores, key=scores.get)


class BacktestResult(BaseModel):
    """Агрегированный результат бэктеста."""

    model_config = ConfigDict(frozen=True)

    # Метрики
    sharpe: float
    max_dd: Decimal  # максимальная просадка, %
    win_rate: Decimal  # доля прибыльных сделок, %
    profit_factor: Decimal  # gross profit / gross loss
    total_trades: int
    total_pnl: Decimal  # итоговый P&L
    avg_trade_pnl: Decimal  # средний P&L на сделку

    # Параметры
    start_capital: Decimal
    end_capital: Decimal

    # Equity curve: list of (timestamp_iso, equity_value)
    equity_curve: list[tuple[str, Decimal]] = Field(default_factory=list)


class Quote(BaseModel):
    """Снимок стакана (top-N уровней)."""

    figi: str
    timestamp: datetime
    bids: list[tuple[Decimal, int]]  # (цена, кол-во)
    asks: list[tuple[Decimal, int]]

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid_price(self) -> Optional[Decimal]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb + ba) / Decimal(2)
