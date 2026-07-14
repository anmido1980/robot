"""Watchdog для long-run paper-trading.

Проверяет heartbeat.json каждые check_interval_seconds и реагирует:
- stale > stale_threshold_seconds → CRITICAL + попытка рестарта longrun.py.
- daily_pnl < -3% от стартового капитала → WARNING.
- daily_pnl < -5% от стартового капитала → CRITICAL (kill-switch должен был сработать).

Использование:
    python 03-scripts/watchdog.py --heartbeat 06-logs/heartbeat.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "02-source"))

from core.alerts import TelegramAlerter  # noqa: E402
from core.clock import MOSCOW_TZ, Clock, SystemClock  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


@dataclass
class WatchdogConfig:
    """Конфигурация watchdog."""

    heartbeat_path: Path = field(default_factory=lambda: Path("06-logs/heartbeat.json"))
    check_interval_seconds: float = 120.0
    stale_threshold_seconds: float = 300.0
    warning_loss_pct: float = 0.03
    critical_loss_pct: float = 0.05
    initial_capital: float = 1_000_000.0
    longrun_command: list[str] = field(
        default_factory=lambda: [sys.executable, str(_PROJECT_ROOT / "05-bots" / "longrun.py"), "--one-session"]
    )
    one_shot: bool = False


@dataclass
class HeartbeatStatus:
    """Распарсенный heartbeat."""

    timestamp: Optional[datetime]
    status: str
    daily_pnl: float
    total_pnl: float
    position_qty: int
    stale_seconds: Optional[float]
    raw: dict[str, Any]

    def is_stale(self, threshold: float = 300.0) -> bool:
        if self.stale_seconds is None:
            return True
        return self.stale_seconds > threshold


class Watchdog:
    """Мониторинг heartbeat и авто-восстановление long-run."""

    def __init__(
        self,
        config: WatchdogConfig,
        clock: Optional[Clock] = None,
    ) -> None:
        self.config = config
        self.clock = clock or SystemClock()
        self._restart_attempts: int = 0
        self._max_restarts_per_hour: int = 3
        self._last_restart: Optional[datetime] = None
        self.alerter = TelegramAlerter()

    # --- Публичный API -------------------------------------------------------

    async def run(self) -> None:
        """Главный цикл watchdog."""
        log.info("Watchdog запущен")
        while True:
            await self._check_once()
            if self.config.one_shot:
                break
            await asyncio.sleep(self.config.check_interval_seconds)

    async def _check_once(self) -> None:
        """Один цикл проверки heartbeat."""
        heartbeat = self._read_heartbeat()

        if heartbeat is None:
            log.error("heartbeat.json не найден или не читается")
            await self._maybe_restart("heartbeat отсутствует")
            return

        # Stale
        if heartbeat.is_stale(self.config.stale_threshold_seconds):
            log.critical(
                f"Heartbeat stale: {heartbeat.stale_seconds:.0f} сек "
                f"> {self.config.stale_threshold_seconds:.0f} сек"
            )
            try:
                await self.alerter.alert_heartbeat_stale(
                    last_seen=heartbeat.timestamp.isoformat() if heartbeat.timestamp else None,
                    stale_seconds=heartbeat.stale_seconds or 0.0,
                )
            except Exception as e:  # noqa: BLE001
                log.warning(f"Ошибка отправки stale alert: {e}")
            await self._maybe_restart("heartbeat stale")
            return

        # Daily P&L thresholds
        daily_loss_pct = self._daily_loss_pct(heartbeat)
        if daily_loss_pct <= -self.config.critical_loss_pct:
            log.critical(
                f"Daily loss {daily_loss_pct:.2%} ≥ "
                f"{self.config.critical_loss_pct:.0%} — kill-switch должен быть активен"
            )
            try:
                from decimal import Decimal

                await self.alerter.alert_kill_switch(
                    reason="daily loss >= 5% (watchdog)",
                    pnl=Decimal(str(heartbeat.daily_pnl)),
                    capital=Decimal(str(self.config.initial_capital)),
                )
            except Exception as e:  # noqa: BLE001
                log.warning(f"Ошибка отправки kill-switch alert: {e}")
        elif daily_loss_pct <= -self.config.warning_loss_pct:
            log.warning(
                f"Daily loss {daily_loss_pct:.2%} ≥ "
                f"{self.config.warning_loss_pct:.0%}"
            )
            try:
                from decimal import Decimal

                await self.alerter.alert_daily_loss_warning(
                    pnl=Decimal(str(heartbeat.daily_pnl)),
                    capital=Decimal(str(self.config.initial_capital)),
                )
            except Exception as e:  # noqa: BLE001
                log.warning(f"Ошибка отправки warning alert: {e}")

        # Runner killed/stopped
        if heartbeat.status in ("killed", "stopped"):
            log.warning(f"Runner status={heartbeat.status}, не рестартуем автоматически")
            return

        log.debug(
            f"OK: status={heartbeat.status}, daily_pnl={heartbeat.daily_pnl:.2f}, "
            f"stale={heartbeat.stale_seconds:.0f}сек"
        )

    # --- Чтение heartbeat ----------------------------------------------------

    def _read_heartbeat(self) -> Optional[HeartbeatStatus]:
        """Прочитать и распарсить heartbeat.json."""
        path = self.config.heartbeat_path
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            log.warning(f"Ошибка чтения heartbeat: {e}")
            return None

        ts = raw.get("timestamp")
        parsed_ts: Optional[datetime] = None
        stale: Optional[float] = None
        if ts:
            try:
                parsed_ts = datetime.fromisoformat(ts)
                stale = (self.clock.now() - parsed_ts).total_seconds()
            except Exception as e:  # noqa: BLE001
                log.warning(f"Ошибка парсинга timestamp heartbeat: {e}")

        return HeartbeatStatus(
            timestamp=parsed_ts,
            status=raw.get("status", "unknown"),
            daily_pnl=float(raw.get("daily_pnl", 0.0)),
            total_pnl=float(raw.get("total_pnl", 0.0)),
            position_qty=int(raw.get("position_qty", 0)),
            stale_seconds=stale,
            raw=raw,
        )

    # --- Рестарт --------------------------------------------------------------

    async def _maybe_restart(self, reason: str) -> None:
        """Перезапустить longrun.py, если не превышен лимит рестартов."""
        now = self.clock.now()
        if self._last_restart is not None:
            if (now - self._last_restart).total_seconds() < 3600:
                self._restart_attempts += 1
            else:
                self._restart_attempts = 1
        else:
            self._restart_attempts = 1
        self._last_restart = now

        if self._restart_attempts > self._max_restarts_per_hour:
            log.error(
                f"Превышен лимит рестартов ({self._max_restarts_per_hour}/час). "
                f"Ручное вмешательство: {reason}"
            )
            try:
                await self.alerter.alert_crash_loop(
                    attempts=self._restart_attempts,
                    last_error=reason,
                )
            except Exception as e:  # noqa: BLE001
                log.warning(f"Ошибка отправки crash-loop alert: {e}")
            return

        log.warning(f"Попытка рестарта longrun ({self._restart_attempts}/3): {reason}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *self.config.longrun_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
            if proc.returncode != 0:
                log.error(
                    f"Рестарт завершился с кодом {proc.returncode}: "
                    f"{stderr.decode(errors='replace')[:200]}"
                )
            else:
                log.info("Рестарт longrun запущен")
        except Exception as e:  # noqa: BLE001
            log.error(f"Ошибка запуска рестарта: {e}")

    # --- Helpers ---------------------------------------------------------------

    def _daily_loss_pct(self, heartbeat: HeartbeatStatus) -> float:
        """Дневной убыток в долях от initial_capital."""
        if self.config.initial_capital <= 0:
            return 0.0
        return heartbeat.daily_pnl / self.config.initial_capital


# --- Точка входа -----------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watchdog для long-run")
    parser.add_argument(
        "--heartbeat",
        type=Path,
        default=Path("06-logs/heartbeat.json"),
        help="Путь к heartbeat.json",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=120.0,
        help="Интервал проверки в секундах",
    )
    parser.add_argument(
        "--stale",
        type=float,
        default=300.0,
        help="Порог stale в секундах",
    )
    parser.add_argument(
        "--one-shot",
        action="store_true",
        help="Одна проверка и выход (для тестов/ cron)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    cfg = WatchdogConfig(
        heartbeat_path=args.heartbeat,
        check_interval_seconds=args.interval,
        stale_threshold_seconds=args.stale,
        one_shot=args.one_shot,
    )
    watchdog = Watchdog(cfg)
    await watchdog.run()


if __name__ == "__main__":
    asyncio.run(main())
