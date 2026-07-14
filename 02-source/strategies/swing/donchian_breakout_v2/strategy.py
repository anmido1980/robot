"""Donchian Breakout v2: Turtle-style пробой + опциональные фильтры.

Базовый канал (как в v1):
- LONG:  close > max(high[:-1], entry_period)
- SHORT: close < min(low[:-1], entry_period)
- Выход: противоположный сигнал (exit_period) ИЛИ ATR-стоп.

Дополнительные фильтры на вход (отключаются флагами):
- ADX: считается на входных (1h/4h) свечах. Только при adx_min > 0.
- Volume: candle.volume >= avg_volume * volume_min_ratio. Только при volume_min_ratio > 0.
- Daily trend: EMA(ema_period) на дневных свечах.
    LONG  -> daily_close >= daily_ema
    SHORT -> daily_close <  daily_ema
  Только при ema_period > 0. Дневные свечи передаются в update_daily_candles()
  один раз (или периодически из скрипта бэктеста).

Все фильтры применяются **только к входу**. Выход (противоположный сигнал,
ATR-стоп) работает всегда, даже если фильтры запретили бы вход.

При `adx_min=0`, `volume_min_ratio=0`, `ema_period=0` стратегия эквивалентна v1.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from core.models import Candle, Side, Signal, Trade
from strategies._shared.indicators import compute_adx, compute_atr, compute_ema


@dataclass(frozen=True)
class DonchianConfigV2:
    """Параметры Donchian Breakout v2.

    Базовые параметры (как в v1):
      entry_period, exit_period, atr_period, stop_atr_mult, strategy_id.

    Фильтры (0 = выключен):
      adx_min          — минимальный ADX (1h/4h) для входа. 0 = без фильтра.
      volume_min_ratio — мин. отношение объёма текущей свечи к среднему за
                         entry_period. 0 = без фильтра.
      ema_period       — период EMA на дневных свечах. 0 = без daily trend.
      adx_period       — период расчёта ADX (default 14).
    """

    entry_period: int = 20
    exit_period: int = 10
    atr_period: int = 14
    stop_atr_mult: float = 2.0
    strategy_id: str = "donchian_breakout_v2"

    # Фильтры
    adx_min: float = 0.0
    adx_period: int = 14
    volume_min_ratio: float = 0.0
    ema_period: int = 0


class _State:
    """Внутреннее состояние стратегии (мутабельное)."""

    __slots__ = (
        "in_position", "position_side", "entry_price", "stop_price",
        "candles", "atr",
        "daily_candles", "daily_ema",
    )

    def __init__(self) -> None:
        self.in_position: bool = False
        self.position_side: Optional[Side] = None
        self.entry_price: Optional[Decimal] = None
        self.stop_price: Optional[Decimal] = None
        self.candles: list[Candle] = []
        self.atr: Optional[Decimal] = None
        self.daily_candles: list[Candle] = []
        self.daily_ema: Optional[Decimal] = None


class DonchianBreakoutV2:
    """Donchian Breakout v2 (с опциональными фильтрами на вход)."""

    def __init__(self, config: Optional[DonchianConfigV2] = None) -> None:
        self._config = config or DonchianConfigV2()
        self._state = _State()

    @property
    def strategy_id(self) -> str:
        return self._config.strategy_id

    def update_daily_candles(self, daily_candles: list[Candle]) -> None:
        """Передать дневные свечи для daily-trend фильтра.

        Дневные свечи должны быть упорядочены по времени и покрывать период
        работы стратегии (минимум ema_period свечей до первого входа).
        Вызывать один раз при инициализации или периодически (в live-режиме)
        по мере поступления новых дневных свечей.
        """
        self._state.daily_candles = list(daily_candles)
        self._state.daily_ema = None
        if self._config.ema_period > 0 and len(daily_candles) >= self._config.ema_period:
            self._state.daily_ema = compute_ema(daily_candles, self._config.ema_period)

    def _refresh_daily_ema(self) -> None:
        """Пересчитать EMA на дневных свечах (если не было вызвано
        update_daily_candles)."""
        cfg = self._config
        st = self._state
        if cfg.ema_period > 0 and st.daily_candles and st.daily_ema is None:
            if len(st.daily_candles) >= cfg.ema_period:
                st.daily_ema = compute_ema(st.daily_candles, cfg.ema_period)

    def _daily_trend_ok(self, candle: Candle, side: Side) -> Optional[bool]:
        """Проверить daily-trend фильтр.

        Returns:
            None — фильтр неактивен (ema_period == 0 или нет дневных свечей).
            True — сигнал в направлении тренда.
            False — сигнал против тренда (отсекаем).
        """
        cfg = self._config
        st = self._state
        if cfg.ema_period <= 0:
            return None
        if not st.daily_candles:
            return None
        self._refresh_daily_ema()
        if st.daily_ema is None:
            return None

        # Найти последнюю дневную свечу с датой <= текущей
        candle_date: date = candle.timestamp.date()
        last_daily: Optional[Candle] = None
        for d in reversed(st.daily_candles):
            if d.timestamp.date() <= candle_date:
                last_daily = d
                break
        if last_daily is None:
            return None  # нет данных за нужный день — пропускаем фильтр

        daily_close = float(last_daily.close)
        ema = float(st.daily_ema)
        if side == Side.BUY:
            return daily_close >= ema
        return daily_close < ema

    def _volume_ok(self, candle: Candle) -> bool:
        """Volume-фильтр: текущий объём >= средний за entry_period * ratio.

        При volume_min_ratio == 0 фильтр неактивен.
        Если накоплено меньше entry_period свечей — тоже неактивен.
        """
        cfg = self._config
        st = self._state
        if cfg.volume_min_ratio <= 0:
            return True
        if len(st.candles) < cfg.entry_period:
            return True
        window = st.candles[-cfg.entry_period:]
        avg = sum(int(c.volume) for c in window) / len(window)
        if avg <= 0:
            return True
        return int(candle.volume) >= avg * cfg.volume_min_ratio

    def _adx_ok(self) -> bool:
        """ADX-фильтр: текущий ADX >= adx_min.

        При adx_min <= 0 фильтр неактивен.
        Если данных недостаточно — пропускаем (не блокируем).
        """
        cfg = self._config
        st = self._state
        if cfg.adx_min <= 0:
            return True
        adx = compute_adx(st.candles, cfg.adx_period)
        if adx is None:
            return True
        return float(adx) >= cfg.adx_min

    def on_candle(self, candle: Candle) -> Optional[Signal]:
        cfg = self._config
        st = self._state
        st.candles.append(candle)
        st.atr = compute_atr(st.candles, cfg.atr_period)

        if len(st.candles) < cfg.entry_period + 1:
            return None

        # Канал по предыдущим N свечам
        window = st.candles[-(cfg.entry_period + 1):-1]
        upper = max(float(c.high) for c in window)
        lower = min(float(c.low) for c in window)
        close = float(candle.close)

        # --- Управление открытой позицией (выходы — без фильтров) ---
        if st.in_position and st.stop_price is not None and st.position_side is not None:
            if st.position_side == Side.BUY and close <= float(st.stop_price):
                return self._close_signal(candle, "stop_long")
            if st.position_side == Side.SELL and close >= float(st.stop_price):
                return self._close_signal(candle, "stop_short")

        if st.in_position:
            if len(st.candles) > cfg.exit_period + 1:
                exit_window = st.candles[-(cfg.exit_period + 1):-1]
                exit_lower = min(float(c.low) for c in exit_window)
                exit_upper = max(float(c.high) for c in exit_window)
                if st.position_side == Side.BUY and close < exit_lower:
                    return self._close_signal(candle, "exit_opposite_long")
                if st.position_side == Side.SELL and close > exit_upper:
                    return self._close_signal(candle, "exit_opposite_short")
            return None

        # --- Вход — с фильтрами ---
        if close > upper:
            if not self._volume_ok(candle) or not self._adx_ok():
                return None
            trend_ok = self._daily_trend_ok(candle, Side.BUY)
            if trend_ok is False:
                return None
            return self._open_signal(candle, Side.BUY, "breakout_up")

        if close < lower:
            if not self._volume_ok(candle) or not self._adx_ok():
                return None
            trend_ok = self._daily_trend_ok(candle, Side.SELL)
            if trend_ok is False:
                return None
            return self._open_signal(candle, Side.SELL, "breakout_down")

        return None

    def _open_signal(self, candle: Candle, side: Side, reason: str) -> Signal:
        st = self._state
        st.in_position = True
        st.position_side = side
        st.entry_price = candle.close
        if st.atr is not None and st.atr > 0:
            stop_dist = float(st.atr) * self._config.stop_atr_mult
            if side == Side.BUY:
                st.stop_price = Decimal(str(round(float(candle.close) - stop_dist, 6)))
            else:
                st.stop_price = Decimal(str(round(float(candle.close) + stop_dist, 6)))
        return Signal(
            figi=candle.figi,
            side=side,
            price=None,
            strategy_id=self.strategy_id,
            reason=reason,
            timestamp=candle.timestamp,
        )

    def _close_signal(self, candle: Candle, reason: str) -> Signal:
        st = self._state
        st.in_position = False
        close_side = Side.SELL if st.position_side == Side.BUY else Side.BUY
        st.position_side = None
        st.entry_price = None
        st.stop_price = None
        return Signal(
            figi=candle.figi,
            side=close_side,
            price=None,
            strategy_id=self.strategy_id,
            reason=reason,
            timestamp=candle.timestamp,
        )

    def on_fill(self, trade: Trade) -> None:
        """No-op для бэктеста; live — обновить entry_price при slippage."""
        pass

    def reset(self) -> None:
        self._state = _State()
