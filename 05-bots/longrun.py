"""Long-run wrapper для PaperTradingRunner.

Управляет жизненным циклом paper-trading сессий:
- Запускает runner в 09:55 МСК (подготовка к открытию рынка 10:00).
- Останавливает в 23:45 МСК (до клиринга 23:50).
- Пропускает выходные и праздники ММВБ.
- При crash делает до 5 попыток с exponential backoff (1, 2, 4, 8, 16 мин).
- При kill-switch останавливается до следующего торгового дня.
- Сохраняет state.json при каждом завершении сессии.

Использование:
    python 05-bots/longrun.py --config 05-bots/donchian_paper.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from loguru import logger

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "02-source"))
sys.path.insert(0, str(_PROJECT_ROOT / "05-bots"))

# Загружаем .env перед импортом broker-конфигурации
load_dotenv(_PROJECT_ROOT / ".env")

# PID-файл для защиты от двойного запуска longrun
_LONGRUN_PID_FILE = _PROJECT_ROOT / "06-logs" / "longrun.pid"


def _is_process_running(pid: int) -> bool:
    """True, если процесс Windows с данным PID существует."""
    kernel32 = ctypes.windll.kernel32
    SYNCHRONIZE = 0x00100000
    handle = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
    if handle:
        kernel32.CloseHandle(handle)
        return True
    return False


def _is_same_command_line(pid: int) -> bool:
    """True, если другой процесс имеет такой же CommandLine (дубль)."""
    try:
        import wmi  # type: ignore
        c = wmi.WMI()
        me = os.getpid()
        my_cmd = None
        for proc in c.Win32_Process(ProcessId=me):
            my_cmd = proc.CommandLine
            break
        if not my_cmd:
            return False
        for proc in c.Win32_Process(ProcessId=pid):
            return proc.CommandLine == my_cmd
    except Exception:
        pass
    return False


class _LongrunPidLock:
    """PID-файл: не допускаем одновременный запуск нескольких longrun."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.acquired = False

    def acquire(self, force: bool = False) -> bool:
        """True = lock захвачен. False = другой процесс уже работает."""
        if self.path.exists() and not force:
            try:
                old_pid = int(self.path.read_text(encoding="utf-8").strip())
                if old_pid != os.getpid() and _is_process_running(old_pid):
                    # Если другой процесс — наш дубль с таким же CommandLine,
                    # считаем, что lock уже захвачен этим же экземпляром.
                    if _is_same_command_line(old_pid):
                        return False
                    return False
            except Exception:
                pass
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(os.getpid()), encoding="utf-8")
        self.acquired = True
        return True

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            if self.path.exists():
                current = int(self.path.read_text(encoding="utf-8").strip())
                if current == os.getpid():
                    self.path.unlink()
        except Exception:
            pass
        self.acquired = False

from core.alerts import TelegramAlerter  # noqa: E402
from core.clock import MOSCOW_TZ, Clock, SystemClock  # noqa: E402
from runner import PaperTradingRunner, RunnerConfig  # noqa: E402


# Жёсткий список праздников ММВБ на июль 2026. Расширяется по мере необходимости.
HOLIDAYS_2026: set[date] = {
    date(2026, 1, 1),
    date(2026, 1, 2),
    date(2026, 1, 3),
    date(2026, 1, 4),
    date(2026, 1, 5),
    date(2026, 1, 6),
    date(2026, 1, 7),
    date(2026, 1, 8),
    date(2026, 2, 23),
    date(2026, 3, 8),
    date(2026, 5, 1),
    date(2026, 5, 4),
    date(2026, 5, 9),
    date(2026, 5, 11),
    date(2026, 6, 12),
    date(2026, 11, 4),
    date(2026, 12, 31),
}


@dataclass
class LongRunConfig:
    """Конфигурация long-run wrapper."""

    runner_config_path: Path = field(default_factory=lambda: Path("05-bots/donchian_paper.yaml"))
    session_start: time = time(9, 55)
    session_end: time = time(23, 45)
    state_path: Path = field(default_factory=lambda: Path("06-logs/state.json"))
    max_retries_per_day: int = 5
    backoff_minutes: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0, 16.0)
    one_session: bool = False  # для smoke: запустить одну сессию и выйти


class LongRunBot:
    """Wrapper для автономного запуска PaperTradingRunner по расписанию."""

    def __init__(
        self,
        long_config: LongRunConfig,
        runner_config: RunnerConfig,
        clock: Optional[Clock] = None,
    ) -> None:
        self.long_config = long_config
        self.runner_config = runner_config
        self.clock = clock or SystemClock()
        self._state: dict[str, Any] = {}
        self._killed_today: bool = False
        self.alerter = TelegramAlerter()

    # --- Публичный API -------------------------------------------------------

    async def run(self) -> None:
        """Главный цикл long-run."""
        if self.long_config.one_session:
            await self._run_single_session()
            return

        logger.info("LongRunBot запущен")
        while True:
            now = self.clock.now()
            if not self._is_trading_day(now):
                await self._wait_until_next_session()
                continue

            if self._killed_today:
                # Kill-switch активирован сегодня — ждём следующего торгового дня
                logger.warning("Kill-switch был активирован сегодня, жду следующего дня")
                self._killed_today = False
                await self._wait_until_next_session()
                continue

            await self._wait_until_session_start(now)

            # Проверяем STOP.flag перед стартом
            if self._stop_flag_exists():
                logger.warning("STOP.flag найден, сессия не запускается")
                self._killed_today = True
                await self._wait_until_next_session()
                continue

            await self._run_session_with_retries()

    async def _run_single_session(self) -> None:
        """Один сеанс (для smoke-тестов)."""
        logger.info("LongRunBot single-session mode")
        await self._run_session_with_retries()

    # --- Расписание ----------------------------------------------------------

    def _is_trading_day(self, now: datetime) -> bool:
        """True, если today — рабочий день (не выходной и не праздник)."""
        today = now.date()
        if today.weekday() >= 5:  # суббота/воскресенье
            return False
        return today not in HOLIDAYS_2026

    def _next_session_start(self, now: datetime) -> datetime:
        """Ближайший session_start (09:55) в рабочий день."""
        candidate = now.replace(hour=self.long_config.session_start.hour,
                                minute=self.long_config.session_start.minute,
                                second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)

        while not self._is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    async def _wait_until_session_start(self, now: datetime) -> None:
        """Спать до следующего session_start."""
        target = self._next_session_start(now)
        delay = (target - now).total_seconds()
        if delay > 0:
            logger.info(f"Ожидание до следующей сессии: {target} ({delay:.0f} сек)")
            await asyncio.sleep(delay)

    async def _wait_until_next_session(self) -> None:
        """Спать до начала следующего торгового дня."""
        now = self.clock.now()
        await self._wait_until_session_start(now)

    # --- Сессия и recovery ---------------------------------------------------

    async def _run_session_with_retries(self) -> None:
        """Запустить одну торговую сессию с retry."""
        if self._stop_flag_exists():
            logger.warning("STOP.flag найден, сессия не запускается")
            self._killed_today = True
            return

        for attempt in range(self.long_config.max_retries_per_day):
            try:
                await self._run_session()
                return
            except Exception as e:  # noqa: BLE001
                if self._stop_flag_exists():
                    logger.warning("STOP.flag найден после падения, останавливаем retry")
                    self._killed_today = True
                    return
                backoff = self.long_config.backoff_minutes[
                    min(attempt, len(self.long_config.backoff_minutes) - 1)
                ]
                logger.exception(
                    f"Сессия упала (попытка {attempt + 1}/{self.long_config.max_retries_per_day}): {e}. "
                    f"Перезапуск через {backoff} мин"
                )
                await asyncio.sleep(backoff * 60)

        logger.error("Исчерпаны все попытки на сегодня")

    async def _run_session(self) -> None:
        """Запустить PaperTradingRunner до session_end или kill-switch."""
        if self._stop_flag_exists():
            logger.warning("STOP.flag найден, сессия не запускается")
            self._killed_today = True
            return

        runner = PaperTradingRunner(self.runner_config)
        runner.clock = self.clock

        # Восстановление состояния (если есть)
        self._load_state(runner)

        logger.info(
            f"Старт сессии: {self.clock.now()}, deadline={self.long_config.session_end}"
        )

        # Установим runtime limit до session_end
        runner.config.max_runtime_minutes = self._minutes_until_session_end()

        try:
            await runner.run()
        finally:
            self._save_state(runner)
            if self._stop_flag_created_during_session():
                logger.warning("Kill-switch сработал во время сессии")
                self._killed_today = True
                try:
                    total_pnl = runner.pnl.total_pnl() if runner.pnl else Decimal(0)
                    await self.alerter.alert_kill_switch(
                        reason="STOP.flag / kill-switch",
                        pnl=total_pnl,
                        capital=runner.initial_capital,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"Ошибка отправки kill-switch alert из longrun: {e}")

    def _minutes_until_session_end(self) -> int:
        """Минут до session_end от текущего времени."""
        now = self.clock.now()
        end = now.replace(hour=self.long_config.session_end.hour,
                          minute=self.long_config.session_end.minute,
                          second=0, microsecond=0)
        if end <= now:
            end += timedelta(days=1)
        return max(1, int((end - now).total_seconds() // 60))

    # --- STOP.flag / kill-switch --------------------------------------------

    def _stop_flag_exists(self) -> bool:
        """Проверить наличие STOP.flag."""
        stop_flag = self.runner_config.log_dir.parent / "STOP.flag"
        return stop_flag.exists()

    def _stop_flag_created_during_session(self) -> bool:
        """True, если STOP.flag существует (kill-switch сработал)."""
        return self._stop_flag_exists()

    # --- State ---------------------------------------------------------------

    def _load_state(self, runner: PaperTradingRunner) -> None:
        """Загрузить state.json в runner (PnL + общий P&L)."""
        path = self.long_config.state_path
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self._state = data
            if runner.pnl is not None and "pnl_state" in data:
                runner.pnl.from_state(data["pnl_state"])
                logger.info(f"PnL state загружен из {path}")
            logger.info(f"State загружен из {path}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Не удалось загрузить state: {e}")

    def _save_state(self, runner: PaperTradingRunner) -> None:
        """Сохранить состояние runner в state.json."""
        path = self.long_config.state_path
        try:
            pnl = runner.pnl
            data: dict[str, Any] = {
                "saved_at": self.clock.now().isoformat(),
                "total_pnl": float(pnl.total_pnl()) if pnl else 0.0,
                "unrealized_pnl": float(pnl.unrealized_pnl()) if pnl else 0.0,
                "daily_realized_pnl": float(pnl.daily_realized_pnl()) if pnl else 0.0,
                "pnl_state": pnl.to_state() if pnl else {},
                "positions": [
                    {
                        "figi": pos.figi,
                        "side": pos.side.value,
                        "quantity": pos.quantity,
                        "average_price": str(pos.average_price),
                    }
                    for pos in (pnl.all_positions() if pnl else [])
                ],
                "stats": runner._stats,
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info(f"State сохранён в {path}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Не удалось сохранить state: {e}")


# --- Точка входа -----------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Long-run wrapper для paper-trading")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("05-bots/donchian_paper.yaml"),
        help="Путь к YAML-конфигу runner'а",
    )
    parser.add_argument(
        "--one-session",
        action="store_true",
        help="Запустить одну сессию и выйти (для smoke)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Игнорировать PID-файл и запустить даже если другой процесс работает",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    lock = _LongrunPidLock(_LONGRUN_PID_FILE)
    if not lock.acquire(force=args.force):
        logger.error(
            f"Другой процесс longrun уже запущен (PID-файл {lock.path}). "
            "Используйте --force для игнорирования."
        )
        sys.exit(1)
    try:
        runner_cfg = RunnerConfig.from_yaml(args.config)
        long_cfg = LongRunConfig(
            runner_config_path=args.config,
            one_session=args.one_session,
        )
        bot = LongRunBot(long_cfg, runner_cfg)
        await bot.run()
    finally:
        lock.release()


if __name__ == "__main__":
    logger.add(
        _PROJECT_ROOT / "06-logs" / "longrun.log",
        rotation="1 day",
        retention="14 days",
        compression="zip",
    )
    asyncio.run(main())
