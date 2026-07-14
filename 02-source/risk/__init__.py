"""Риск-менеджер и kill-switch.

Все заявки проходят через RiskManager перед отправкой на биржу.
Kill-switch — экстренная остановка торговли.
"""

from .config import RiskConfig
from .kill_switch import KillSwitch
from .manager import RiskManager, RiskViolation

__all__ = ["RiskConfig", "RiskManager", "RiskViolation", "KillSwitch"]