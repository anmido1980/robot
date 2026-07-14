"""Ежедневная сверка paper-P&L с бэктестом.

Сравнивает cumulative paper P&L из ежедневных отчётов `01-docs/journal/YYYY-MM-DD.md`
с кривой эквити бэктеста (CSV). При расхождении > threshold шлёт Telegram-алерт.

Использование:
    python 03-scripts/compare_backtest.py --date 2026-07-06
        --journal-dir 01-docs/journal
        --backtest-equity 06-logs/backtest/donchian_v2_equity.csv
        --initial-capital 200000
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "02-source"))

from core.alerts import TelegramAlerter, TelegramConfig  # noqa: E402

REPORT_DIR = ROOT / "04-output"
JOURNAL_DIR = ROOT / "01-docs" / "journal"
BACKTEST_EQUITY_DEFAULT = ROOT / "06-logs" / "backtest" / "donchian_v2_equity.csv"
DIVERGENCE_THRESHOLD_DEFAULT = Decimal("0.30")

TOTAL_PNL_RE = re.compile(r"\|\s*Общий\s+P&L\s*\|\s*([+-]?[\d\s,]+\.?\d*)\s*\|")
REALIZED_PNL_RE = re.compile(r"\|\s*Реализованный\s+P&L\s*\|\s*([+-]?[\d\s,]+\.?\d*)\s*\|")


class Status:
    OK = "OK"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


def _parse_money(value: str) -> Decimal:
    """Парсит сумму вида '1 234.56' или '1,234.56' в Decimal."""
    cleaned = value.replace(" ", "").replace(",", "").replace("₽", "").strip()
    return Decimal(cleaned) if cleaned else Decimal(0)


def _extract_pnl_from_journal(path: Path) -> Optional[Decimal]:
    """Извлекает 'Общий P&L' из Markdown-отчёта."""
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    match = TOTAL_PNL_RE.search(text)
    if not match:
        return None
    return _parse_money(match.group(1))


@dataclass
class EquityPoint:
    date: date
    equity: Decimal
    pnl: Decimal = field(default_factory=Decimal)


@dataclass
class DivergenceResult:
    date: date
    paper_total_pnl: Decimal
    backtest_total_pnl: Decimal
    divergence_pct: Decimal
    status: str
    days_compared: int
    message: str


def load_backtest_equity(path: Path) -> list[EquityPoint]:
    """Загружает CSV с колонками date, equity[, pnl]."""
    if not path.exists():
        raise FileNotFoundError(f"Backtest equity CSV не найден: {path}")
    rows: list[EquityPoint] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = datetime.strptime(row["date"], "%Y-%m-%d").date()
            equity = _parse_money(row["equity"])
            pnl = _parse_money(row.get("pnl", "0"))
            rows.append(EquityPoint(date=d, equity=equity, pnl=pnl))
    rows.sort(key=lambda x: x.date)
    return rows


def build_paper_curve(journal_dir: Path, until: date) -> list[EquityPoint]:
    """Собирает paper equity curve из ежедневных отчётов до указанной даты."""
    points: list[EquityPoint] = []
    for path in sorted(journal_dir.glob("*.md")):
        name = path.stem
        try:
            d = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d > until:
            continue
        pnl = _extract_pnl_from_journal(path)
        if pnl is None:
            continue
        points.append(EquityPoint(date=d, equity=Decimal(0), pnl=pnl))
    points.sort(key=lambda x: x.date)
    return points


def _interpolate_backtest_pnl(
    backtest: list[EquityPoint], target_date: date, initial_capital: Decimal
) -> Decimal:
    """Линейная интерполяция cumulative P&L бэктеста на target_date.

    CSV колонка `equity` — это total equity (initial + P&L). Возвращаем P&L.
    """
    if not backtest:
        return Decimal(0)
    first = backtest[0]
    last = backtest[-1]
    if target_date <= first.date:
        return first.equity - initial_capital
    if target_date >= last.date:
        return last.equity - initial_capital

    # Ищем точку не позже target_date
    prev = first
    for pt in backtest:
        if pt.date > target_date:
            break
        prev = pt
    next_pt = next((p for p in backtest if p.date > prev.date), last)
    if prev.date == next_pt.date:
        return prev.equity - initial_capital

    total_days = (next_pt.date - prev.date).days
    elapsed = (target_date - prev.date).days
    ratio = Decimal(elapsed) / Decimal(total_days)
    interpolated_equity = prev.equity + (next_pt.equity - prev.equity) * ratio
    return interpolated_equity - initial_capital


def compare_pnl(
    target_date: date,
    journal_dir: Path,
    backtest_equity: Path,
    initial_capital: Decimal,
    threshold: Decimal,
) -> DivergenceResult:
    """Сравнивает paper и backtest P&L на target_date."""
    backtest = load_backtest_equity(backtest_equity)
    paper = build_paper_curve(journal_dir, target_date)

    if not paper:
        return DivergenceResult(
            date=target_date,
            paper_total_pnl=Decimal(0),
            backtest_total_pnl=Decimal(0),
            divergence_pct=Decimal(0),
            status=Status.CRITICAL,
            days_compared=0,
            message="Нет ежедневных отчётов journal для сравнения",
        )

    paper_total = paper[-1].pnl
    backtest_total = _interpolate_backtest_pnl(backtest, target_date, initial_capital)

    if backtest_total == 0:
        divergence_pct = Decimal(0) if paper_total == 0 else Decimal("1.0")
    else:
        divergence_pct = (paper_total - backtest_total) / abs(backtest_total)

    abs_div = abs(divergence_pct)
    if abs_div >= threshold:
        status = Status.CRITICAL
    elif abs_div >= threshold / 2:
        status = Status.WARN
    else:
        status = Status.OK

    message = (
        f"paper={paper_total:.2f}, backtest={backtest_total:.2f}, "
        f"divergence={divergence_pct*100:.1f}%, days={len(paper)}"
    )

    return DivergenceResult(
        date=target_date,
        paper_total_pnl=paper_total,
        backtest_total_pnl=backtest_total,
        divergence_pct=divergence_pct,
        status=status,
        days_compared=len(paper),
        message=message,
    )


def _write_report(result: DivergenceResult, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{result.date.isoformat()}_backtest_compare.md"

    if result.status == Status.OK:
        emoji = "✅"
    elif result.status == Status.WARN:
        emoji = "⚠️"
    else:
        emoji = "🚨"

    lines = [
        f"# Сверка paper-P&L с бэктестом — {result.date}",
        "",
        f"**Статус:** {emoji} {result.status}",
        f"**Дней в сравнении:** {result.days_compared}",
        "",
        "| Метрика | Значение |",
        "|---------|----------|",
        f"| Paper cumulative P&L | {result.paper_total_pnl:.2f} |",
        f"| Backtest expected P&L | {result.backtest_total_pnl:.2f} |",
        f"| Расхождение | {result.divergence_pct*100:.1f}% |",
        f"| Порог | {DIVERGENCE_THRESHOLD_DEFAULT*100:.0f}% |",
        "",
        f"**Детали:** {result.message}",
        "",
        "## Критерий перехода в live",
        "",
        f"- [ ] {'Расхождение ≤ 30% за 4 недели' if result.status != Status.CRITICAL else 'Расхождение > 30% — long-run остановить, искать причину'}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


async def _maybe_alert(result: DivergenceResult) -> None:
    if result.status != Status.CRITICAL:
        return
    cfg = TelegramConfig.from_env()
    alerter = TelegramAlerter(cfg)
    try:
        await alerter.alert_backtest_divergence(
            paper_pnl=result.paper_total_pnl,
            expected_pnl=result.backtest_total_pnl,
            divergence_pct=result.divergence_pct * 100,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[compare_backtest] Алерт не отправлен: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Сверка paper-P&L с бэктестом")
    parser.add_argument("--date", required=True, help="Дата сравнения YYYY-MM-DD")
    parser.add_argument("--journal-dir", type=Path, default=JOURNAL_DIR)
    parser.add_argument("--backtest-equity", type=Path, default=BACKTEST_EQUITY_DEFAULT)
    parser.add_argument("--initial-capital", type=Decimal, default=Decimal("200000"))
    parser.add_argument("--alert-threshold", type=Decimal, default=DIVERGENCE_THRESHOLD_DEFAULT)
    parser.add_argument("--output-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--no-alert", action="store_true", help="Не отправлять Telegram-алерт")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    try:
        result = compare_pnl(
            target_date=target_date,
            journal_dir=args.journal_dir,
            backtest_equity=args.backtest_equity,
            initial_capital=args.initial_capital,
            threshold=args.alert_threshold,
        )
    except FileNotFoundError as exc:
        print(f"[compare_backtest] FAIL: {exc}")
        return 2

    path = _write_report(result, args.output_dir)
    print(f"Status: {result.status}")
    print(f"Report: {path}")
    print(result.message)

    if result.status == Status.CRITICAL and not args.no_alert:
        asyncio.run(_maybe_alert(result))

    return 0 if result.status != Status.CRITICAL else 1


if __name__ == "__main__":
    raise SystemExit(main())
