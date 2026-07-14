"""Генерация ежедневного Markdown-отчёта.

Формат: 01-docs/journal/YYYY-MM-DD.md
Содержимое: дата, P&L за день, список сделок, открытые позиции, метрики.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from core.clock import MOSCOW_TZ
from core.models import Trade

from .pnl import PnLCalculator
from .trades import TradeJournal

log = logging.getLogger(__name__)

# Шаблон отчёта
_REPORT_TEMPLATE = """# Торговый отчёт {date}

## Сводка

| Метрика | Значение |
|---------|----------|
| Реализованный P&L | {realized_pnl} |
| Нереализованный P&L | {unrealized_pnl} |
| Общий P&L | {total_pnl} |
| Кол-во сделок | {trade_count} |

## Открытые позиции

{positions_table}

## Сделки за день

{trades_table}

---
*Отчёт создан автоматически в {created_at}*
"""


class DailyReport:
    """Генерация ежедневного отчёта в Markdown.

    Использование:
        report = DailyReport(journal_dir="01-docs/journal")
        path = report.generate(date.today(), journal, pnl)
    """

    def __init__(self, journal_dir: str | Path = "01-docs/journal") -> None:
        self._journal_dir = Path(journal_dir)

    def generate(
        self,
        report_date: date,
        journal: TradeJournal,
        pnl: PnLCalculator,
    ) -> Path:
        """Сгенерировать и сохранить отчёт.

        Returns:
            Путь к созданному файлу
        """
        self._journal_dir.mkdir(parents=True, exist_ok=True)

        # Получаем сделки за день
        trades = journal.get_trades(report_date)

        # P&L
        realized = pnl.realized_pnl()
        unrealized = pnl.unrealized_pnl()
        total = pnl.total_pnl()

        # Формируем таблицы
        positions_table = self._format_positions(pnl)
        trades_table = self._format_trades(trades)

        # Заполняем шаблон
        content = _REPORT_TEMPLATE.format(
            date=report_date.isoformat(),
            realized_pnl=self._fmt_decimal(realized),
            unrealized_pnl=self._fmt_decimal(unrealized),
            total_pnl=self._fmt_decimal(total),
            trade_count=len(trades),
            positions_table=positions_table,
            trades_table=trades_table,
            created_at=datetime.now(MOSCOW_TZ).isoformat(),
        )

        # Сохраняем
        path = self._journal_dir / f"{report_date.isoformat()}.md"
        path.write_text(content, encoding="utf-8")
        log.info("Отчёт сохранён: %s", path)
        return path

    @staticmethod
    def _format_positions(pnl: PnLCalculator) -> str:
        """Таблица открытых позиций."""
        positions = pnl.all_positions()
        if not positions:
            return "_Нет открытых позиций_"

        lines = ["| FIGI | Сторона | Кол-во | Ср. цена | Тек. цена | P&L |"]
        lines.append("|------|---------|--------|----------|-----------|-----|")
        for pos in positions:
            lines.append(
                f"| {pos.figi} | {pos.side.value} | {pos.quantity} | "
                f"{pos.average_price:.2f} | "
                f"{pos.current_price or '—'} | "
                f"{pos.unrealized_pnl:.2f} |"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_trades(trades: list[Trade]) -> str:
        """Таблица сделок."""
        if not trades:
            return "_Нет сделок за день_"

        lines = ["| Время | FIGI | Сторона | Кол-во | Цена |"]
        lines.append("|-------|------|---------|--------|------|")
        for t in trades:
            time_str = t.timestamp.strftime("%H:%M:%S")
            lines.append(
                f"| {time_str} | {t.figi} | {t.side.value} | "
                f"{t.quantity} | {t.price:.2f} |"
            )
        return "\n".join(lines)

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        """Форматирование Decimal для отчёта."""
        if value == Decimal(0):
            return "0.00"
        return f"{value:.2f}"