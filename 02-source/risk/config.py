"""Конфигурация риск-менеджера.

Все лимиты — в Decimal, чтобы избежать ошибок округления float.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class RiskConfig(BaseModel):
    """Параметры риск-менеджера.

    Значения по умолчанию соответствуют CLAUDE.md:
    - позиция ≤ 50% депо
    - дневной убыток ≤ 5%
    - макс. кол-во позиций = 5
    - стоп на позицию — опционально
    """

    model_config = {"frozen": True}

    # Лимиты
    max_position_pct: Decimal = Field(
        default=Decimal("0.5"),
        description="Максимальная доля депозита на одну позицию (0.5 = 50%)",
    )
    max_daily_loss_pct: Decimal = Field(
        default=Decimal("0.05"),
        description="Максимальный дневной убыток от стартового портфеля (0.05 = 5%)",
    )
    max_positions: int = Field(
        default=5,
        description="Максимальное количество одновременных позиций",
    )
    stop_loss_pct: Optional[Decimal] = Field(
        default=None,
        description="Стоп-лосс на позицию (% от средней цены). None = без стопа",
    )

    # Kill-switch
    stop_flag_path: Path = Field(
        default=Path("STOP.flag"),
        description="Путь к файлу-флагу для ручного kill-switch",
    )

    # Клиринг
    clearing_hour: int = Field(
        default=0,
        description="Час клиринга (МСК), после которого сбрасывается счётчик дневного P&L",
    )
    clearing_minute: int = Field(
        default=30,
        description="Минута клиринга (МСК). По умолчанию 00:30",
    )