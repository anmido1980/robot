"""Журнал сделок: запись в SQLite, чтение, подписка на EventBus.

SQLite выбран как встраиваемая БД — не требует отдельного сервера,
удобна для аналитики (SQL-запросы) и резервного копирования.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from core.events import EventBus, OrderStateEvent
from core.models import OrderState, Side, Trade

log = logging.getLogger(__name__)

# Схема таблицы trades
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id         TEXT PRIMARY KEY,
    order_request_id TEXT NOT NULL,
    account_id       TEXT NOT NULL,
    figi             TEXT NOT NULL,
    side             TEXT NOT NULL,
    quantity         INTEGER NOT NULL,
    price            TEXT NOT NULL,
    timestamp        TEXT NOT NULL,
    strategy_id      TEXT,
    commission       TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_trades_figi ON trades(figi);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_account ON trades(account_id);
"""


class TradeJournal:
    """Журнал сделок с записью в SQLite.

    Использование:
        journal = TradeJournal("trades.db", event_bus)
        # Подписка на OrderStateEvent автоматически записывает fills
        # Или вручную:
        journal.record(trade)
    """

    def __init__(self, db_path: str | Path, event_bus: Optional[EventBus] = None) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._event_bus = event_bus

        if event_bus is not None:
            event_bus.subscribe("order_state", self._on_order_state)

    # --- Жизненный цикл ------------------------------------------------------

    def connect(self) -> None:
        """Открыть соединение с БД и создать таблицу."""
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_CREATE_TABLE_SQL)
        self._conn.commit()
        log.info("TradeJournal подключён к %s", self._db_path)

    def close(self) -> None:
        """Закрыть соединение."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "TradeJournal":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # --- Запись ---------------------------------------------------------------

    def record(self, trade: Trade, strategy_id: Optional[str] = None, commission: Optional[Decimal] = None) -> None:
        """Записать сделку в журнал."""
        if self._conn is None:
            raise RuntimeError("TradeJournal не подключён — вызовите connect()")

        self._conn.execute(
            """
            INSERT OR IGNORE INTO trades
                (trade_id, order_request_id, account_id, figi, side,
                 quantity, price, timestamp, strategy_id, commission)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.trade_id,
                trade.order_request_id,
                trade.account_id,
                trade.figi,
                trade.side.value,
                trade.quantity,
                str(trade.price),
                trade.timestamp.isoformat(),
                strategy_id,
                str(commission) if commission is not None else None,
            ),
        )
        self._conn.commit()
        log.debug("Записана сделка %s: %s %s x%d @ %s",
                   trade.trade_id, trade.side.value, trade.figi,
                   trade.quantity, trade.price)

    def has_trade(self, trade_id: str) -> bool:
        """Проверить, есть ли сделка в журнале по trade_id."""
        if self._conn is None:
            raise RuntimeError("TradeJournal не подключён")

        row = self._conn.execute(
            "SELECT 1 FROM trades WHERE trade_id = ? LIMIT 1", (trade_id,)
        ).fetchone()
        return row is not None

    # --- Чтение ---------------------------------------------------------------

    def get_trades(self, trade_date: date) -> list[Trade]:
        """Получить все сделки за указанную дату (МСК)."""
        if self._conn is None:
            raise RuntimeError("TradeJournal не подключён")

        start = f"{trade_date.isoformat()}T00:00:00+03:00"
        end = f"{trade_date.isoformat()}T23:59:59+03:00"

        rows = self._conn.execute(
            """
            SELECT trade_id, order_request_id, account_id, figi, side,
                   quantity, price, timestamp
            FROM trades
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """,
            (start, end),
        ).fetchall()

        return [
            Trade(
                trade_id=row[0],
                order_request_id=row[1],
                account_id=row[2],
                figi=row[3],
                side=Side(row[4]),
                quantity=row[5],
                price=Decimal(row[6]),
                timestamp=datetime.fromisoformat(row[7]),
            )
            for row in rows
        ]

    def get_all_trades(self) -> list[Trade]:
        """Получить все сделки из журнала."""
        if self._conn is None:
            raise RuntimeError("TradeJournal не подключён")

        rows = self._conn.execute(
            """
            SELECT trade_id, order_request_id, account_id, figi, side,
                   quantity, price, timestamp
            FROM trades
            ORDER BY timestamp
            """
        ).fetchall()

        return [
            Trade(
                trade_id=row[0],
                order_request_id=row[1],
                account_id=row[2],
                figi=row[3],
                side=Side(row[4]),
                quantity=row[5],
                price=Decimal(row[6]),
                timestamp=datetime.fromisoformat(row[7]),
            )
            for row in rows
        ]

    def get_trades_since(self, since: datetime) -> list[Trade]:
        """Получить сделки с timestamp >= since."""
        if self._conn is None:
            raise RuntimeError("TradeJournal не подключён")

        rows = self._conn.execute(
            """
            SELECT trade_id, order_request_id, account_id, figi, side,
                   quantity, price, timestamp
            FROM trades
            WHERE timestamp >= ?
            ORDER BY timestamp
            """,
            (since.isoformat(),),
        ).fetchall()

        return [
            Trade(
                trade_id=row[0],
                order_request_id=row[1],
                account_id=row[2],
                figi=row[3],
                side=Side(row[4]),
                quantity=row[5],
                price=Decimal(row[6]),
                timestamp=datetime.fromisoformat(row[7]),
            )
            for row in rows
        ]

    # --- События --------------------------------------------------------------

    async def _on_order_state(self, event: OrderStateEvent) -> None:
        """Подписка на OrderStateEvent — запись fills.

        Когда заявка исполняется (FILLED/PARTIALLY_FILLED), создаём Trade.
        FIGI и side берём в приоритете из полей OrderStateInfo (SDK
        OrderState/operation их теперь содержит), fallback — парсинг comment
        runner'а в формате 'runner|figi=FUTSI|side=buy'.
        """
        if event.payload is None:
            return

        state_info = event.payload
        if state_info.state not in (OrderState.FILLED, OrderState.PARTIALLY_FILLED):
            return
        if state_info.filled_quantity <= 0:
            return
        if state_info.average_price is None:
            log.warning(
                "Пропуск сделки без цены: order=%s",
                state_info.order_request_id,
            )
            return

        figi = state_info.figi
        side = state_info.side
        if figi is None or side is None:
            figi, side = self._parse_runner_comment(state_info.comment)

        if figi is None or side is None:
            log.warning(
                "Пропуск сделки без figi/side: order=%s comment=%r figi=%s side=%s",
                state_info.order_request_id, state_info.comment, figi, side,
            )
            return

        trade = Trade(
            trade_id=f"fill-{state_info.order_request_id}-{state_info.timestamp.isoformat()}",
            order_request_id=state_info.order_request_id,
            account_id=state_info.account_id,
            figi=figi,
            side=side,
            quantity=state_info.filled_quantity,
            price=state_info.average_price,
            timestamp=state_info.timestamp,
        )
        self.record(trade)

    @staticmethod
    def _parse_runner_comment(comment: Optional[str]) -> tuple[Optional[str], Optional[Side]]:
        """Извлечь figi и side из comment runner'а.

        Формат: 'runner|figi=FUTSI|side=buy'
        """
        if not comment:
            return None, None
        figi: Optional[str] = None
        side: Optional[Side] = None
        for part in comment.split("|"):
            if part.startswith("figi="):
                figi = part.split("=", 1)[1]
            elif part.startswith("side="):
                try:
                    side = Side(part.split("=", 1)[1])
                except ValueError:
                    pass
        return figi, side