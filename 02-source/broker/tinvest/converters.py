"""Общие конвертеры T-Invest protobuf → core-модели.

Используются в market_data, orders, portfolio. Вынесены сюда,
чтобы избежать дублирования и циклических импортов.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Union

from core.clock import MOSCOW_TZ
from core.models import Side


def quotation_to_decimal(q) -> Decimal:
    """MoneyValue/Quotation → Decimal. SDK использует units + nano.

    В T-Invest units и nano всегда одного знака. Берём знак из units.
    """
    if q is None:
        return Decimal(0)
    units = getattr(q, "units", 0)
    nano = getattr(q, "nano", 0)
    sign = -1 if units < 0 else 1
    return Decimal(sign * abs(units)) + Decimal(nano) / Decimal(1_000_000_000)


def tinkoff_time(dt: Union[datetime, object]) -> datetime:
    """Маппинг tinkoff Timestamp → datetime c МСК-tz.

    SDK возвращает google.protobuf.Timestamp или datetime — нормализуем.
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=MOSCOW_TZ)
        return dt.astimezone(MOSCOW_TZ)
    # google.protobuf.Timestamp
    return dt.ToDatetime().replace(tzinfo=timezone.utc).astimezone(MOSCOW_TZ)


def tinkoff_side_to_side(direction) -> Side:
    """Маппинг tinkoff.invest TradeDirection / OrderDirection → core.Side."""
    name = getattr(direction, "name", str(direction))
    if "BUY" in name.upper():
        return Side.BUY
    if "SELL" in name.upper():
        return Side.SELL
    raise ValueError(f"Неизвестное направление: {direction}")


def execution_status_to_order_state(status) -> str:
    """Маппинг ExecutionReportStatus (T-Invest) → core.OrderState.

    Коды ExecutionReportStatus в SDK:
      EXECUTION_REPORT_STATUS_NEW
      EXECUTION_REPORT_STATUS_FILL (не FILLED!)
      EXECUTION_REPORT_STATUS_PARTIALLYFILL (не PARTIALLY_FILLED!)
      EXECUTION_REPORT_STATUS_CANCELLED
      EXECUTION_REPORT_STATUS_REJECTED
    """
    name = getattr(status, "name", str(status)).upper()
    # Проверяем более длинные/специфичные совпадения первыми
    if "PARTIALLYFILL" in name or "PARTIALLY_FILL" in name or "PARTIALLY_FILLED" in name:
        return "partially_filled"
    if "FILL" in name:
        return "filled"
    if "NEW" in name:
        return "new"
    if "CANCELLED" in name or "CANCELED" in name:
        return "cancelled"
    if "REJECTED" in name:
        return "rejected"
    if "PENDING" in name:
        return "pending"
    if "EXPIRED" in name:
        return "expired"
    # Fallback
    return name.lower()


__all__ = [
    "quotation_to_decimal",
    "tinkoff_time",
    "tinkoff_side_to_side",
    "execution_status_to_order_state",
]