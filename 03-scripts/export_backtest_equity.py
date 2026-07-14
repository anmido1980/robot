"""Экспорт daily equity curve бэктеста Donchian v2 (S1 + F1_ADX15, Si/1h).

Сохраняет CSV для `03-scripts/compare_backtest.py`:
    06-logs/backtest/donchian_v2_equity.csv

Колонки:
    date — YYYY-MM-DD
    equity — total equity (initial_capital + cumulative P&L)
    pnl — cumulative P&L
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "02-source"))

from backtest.data_loader import load_from_parquet  # noqa: E402
from backtest.engine import BacktestConfig, BacktestEngine  # noqa: E402
from backtest.periods import bars_per_year  # noqa: E402
from core.models import Candle  # noqa: E402
from strategies.swing.donchian_breakout_v2.strategy import (  # noqa: E402
    DonchianBreakoutV2,
    DonchianConfigV2,
)

CACHE_DIR = Path("06-logs/candles")
OUTPUT_DIR = Path("06-logs/backtest")
OUTPUT_FILE = OUTPUT_DIR / "donchian_v2_equity.csv"

TICKER_COMMISSION = Decimal("1.5")
TICKER_TICK_VALUE = Decimal("1")
INITIAL_CAPITAL = Decimal("200000")


def load_si_1h() -> list[Candle]:
    path = CACHE_DIR / "Si_swing_1h.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Кеш Si/1h не найден: {path}")
    return load_from_parquet(str(path), figi="FUTSI")


def run_backtest(candles: list[Candle]) -> BacktestEngine:
    cfg = DonchianConfigV2(
        entry_period=20,
        exit_period=10,
        atr_period=14,
        stop_atr_mult=2.0,
        adx_min=15.0,
        adx_period=14,
        volume_min_ratio=0.0,
        ema_period=0,
        strategy_id="donchian_v2_export",
    )
    strategy = DonchianBreakoutV2(cfg)
    bt_cfg = BacktestConfig(
        initial_capital=INITIAL_CAPITAL,
        commission_per_lot=TICKER_COMMISSION,
        slippage_ticks=1,
        tick_value=TICKER_TICK_VALUE,
        bars_per_year=bars_per_year("1h"),
    )
    engine = BacktestEngine(strategy, bt_cfg)
    return engine.run(candles)


def resample_daily(equity_curve: list[tuple[str, Decimal]]) -> list[tuple[str, Decimal, Decimal]]:
    """Берём equity на закрытие каждого дня."""
    df = pd.DataFrame(
        [(datetime.fromisoformat(ts), float(value)) for ts, value in equity_curve],
        columns=["timestamp", "equity"],
    )
    df["date"] = df["timestamp"].dt.date
    daily = df.groupby("date").last().reset_index()
    rows: list[tuple[str, Decimal, Decimal]] = []
    for _, row in daily.iterrows():
        date_str = row["date"].isoformat()
        equity = Decimal(str(row["equity"]))
        pnl = equity - INITIAL_CAPITAL
        rows.append((date_str, equity, pnl))
    return rows


def write_csv(rows: list[tuple[str, Decimal, Decimal]]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "equity", "pnl"])
        for date_str, equity, pnl in rows:
            writer.writerow([date_str, f"{equity:.2f}", f"{pnl:.2f}"])
    return OUTPUT_FILE


def main() -> int:
    print("Загрузка Si/1h Parquet...")
    candles = load_si_1h()
    print(f"Свечей: {len(candles)}")

    print("Запуск бэктеста Donchian v2 (S1 + F1_ADX15)...")
    result = run_backtest(candles)
    print(
        f"Результат: trades={result.total_trades}, total_pnl={result.total_pnl}, "
        f"sharpe={result.sharpe}, max_dd={result.max_dd}%"
    )

    print("Ресемплинг в daily equity...")
    daily = resample_daily(result.equity_curve)
    print(f"Дневных точек: {len(daily)}")

    path = write_csv(daily)
    print(f"Сохранено: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
