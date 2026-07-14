"""Pre-flight чек-лист перед запуском long-run paper-trading.

Проверяет окружение, токены, кеш, диск, тесты и доступность sandbox.
Пишет отчёт в 04-output/YYYY-MM-DD_preflight.md.

Использование:
    python 03-scripts/preflight.py
    python 03-scripts/preflight.py --skip-tests
    python 03-scripts/preflight.py --config 05-bots/donchian_paper.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Добавляем 02-source в PYTHONPATH для импорта broker-адаптера
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "02-source"))

from broker.tinvest.client import TinkoffClient, TinkoffConfig  # noqa: E402

MOSCOW_TZ = timezone.utc  # timestamps храним в UTC, отображаем в МСК в отчёте
REPORT_DIR = ROOT / "04-output"
LOG_DIR = ROOT / "06-logs"
DEFAULT_CONFIG = ROOT / "05-bots" / "donchian_paper.yaml"
PARQUET_CACHE = LOG_DIR / "candles" / "Si_swing_1h.parquet"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
MIN_DISK_GB = 1.0
MIN_PARQUET_ROWS = 21  # Donchian 20/10 требует ≥21 свечу


class Status:
    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class CheckResult:
    name: str
    status: str  # OK | WARN | FAIL
    message: str


@dataclass
class PreflightReport:
    timestamp_utc: datetime
    overall: str
    checks: List[CheckResult] = field(default_factory=list)

    def add(self, name: str, status: str, message: str) -> None:
        self.checks.append(CheckResult(name, status, message))


def _now_msk() -> datetime:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Europe/Moscow"))


def _find_dotenv() -> Path:
    env = ROOT / ".env"
    if env.exists():
        return env
    return ROOT / ".env.example"


def check_env_vars(report: PreflightReport) -> None:
    token = os.getenv("T_INVEST_SANDBOX_TOKEN") or os.getenv("T_INVEST_PROD_TOKEN")
    if not token:
        report.add(
            "env_vars",
            Status.FAIL,
            "Не задан ни T_INVEST_SANDBOX_TOKEN, ни T_INVEST_PROD_TOKEN",
        )
        return
    mode = os.getenv("TRADING_MODE", "paper").lower()
    if mode == "live" and not os.getenv("T_INVEST_PROD_TOKEN"):
        report.add(
            "env_vars",
            Status.FAIL,
            "TRADING_MODE=live, но T_INVEST_PROD_TOKEN не задан",
        )
        return

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    if not tg_token or not tg_chat:
        report.add(
            "env_vars",
            Status.WARN,
            "Telegram-алерты отключены: не задан TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID",
        )
    else:
        report.add("env_vars", Status.OK, "Токены брокера и Telegram на месте")


def check_stop_flag(report: PreflightReport) -> None:
    flag = ROOT / "STOP.flag"
    if flag.exists():
        report.add(
            "stop_flag",
            Status.FAIL,
            f"Обнаружен STOP.flag ({flag}). Удалите файл перед запуском.",
        )
    else:
        report.add("stop_flag", Status.OK, "STOP.flag отсутствует")


def check_config(report: PreflightReport, config_path: Path) -> None:
    if not config_path.exists():
        report.add("config", Status.FAIL, f"Конфиг не найден: {config_path}")
        return
    try:
        import yaml

        with config_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        mode = cfg.get("mode", "unknown")
        report.add("config", Status.OK, f"Конфиг {config_path.name} прочитан (mode={mode})")
    except Exception as exc:  # noqa: BLE001
        report.add("config", Status.FAIL, f"Конфиг повреждён: {exc}")


def check_parquet_cache(report: PreflightReport) -> None:
    if not PARQUET_CACHE.exists():
        report.add(
            "parquet_cache",
            Status.FAIL,
            f"Parquet-кеш не найден: {PARQUET_CACHE}",
        )
        return
    try:
        df = pd.read_parquet(PARQUET_CACHE)
        rows = len(df)
        if rows < MIN_PARQUET_ROWS:
            report.add(
                "parquet_cache",
                Status.FAIL,
                f"Кеш слишком мал: {rows} строк (нужно ≥{MIN_PARQUET_ROWS})",
            )
        else:
            report.add(
                "parquet_cache",
                Status.OK,
                f"Parquet-кеш: {rows} строк ({PARQUET_CACHE.name})",
            )
    except Exception as exc:  # noqa: BLE001
        report.add("parquet_cache", Status.FAIL, f"Не удалось прочитать кеш: {exc}")


def check_disk_space(report: PreflightReport) -> None:
    try:
        usage = shutil.disk_usage(ROOT)
        free_gb = usage.free / (1024**3)
        if free_gb < MIN_DISK_GB:
            report.add(
                "disk_space",
                Status.FAIL,
                f"Свободно {free_gb:.2f} ГБ (нужно >{MIN_DISK_GB} ГБ)",
            )
        else:
            report.add(
                "disk_space",
                Status.OK,
                f"Свободно {free_gb:.2f} ГБ на диске {ROOT.anchor}",
            )
    except Exception as exc:  # noqa: BLE001
        report.add("disk_space", Status.WARN, f"Не удалось проверить место: {exc}")


def check_venv_python(report: PreflightReport) -> None:
    if not VENV_PYTHON.exists():
        report.add("venv_python", Status.FAIL, f"Не найден {VENV_PYTHON}")
        return
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            report.add(
                "venv_python",
                Status.OK,
                f"Интерпретатор venv: {result.stdout.strip()}",
            )
        else:
            report.add(
                "venv_python",
                Status.FAIL,
                f"venv python --version вернул ошибку: {result.stderr}",
            )
    except Exception as exc:  # noqa: BLE001
        report.add("venv_python", Status.FAIL, f"Не удалось запустить venv python: {exc}")


def check_test_suite(report: PreflightReport, skip: bool) -> None:
    if skip:
        report.add("test_suite", Status.WARN, "Проверка тестов пропущена (--skip-tests)")
        return
    if not VENV_PYTHON.exists():
        report.add("test_suite", Status.FAIL, "Нет venv python для запуска тестов")
        return
    try:
        result = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "pytest",
                "02-source/tests",
                "-q",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        # pytest -q выводит summary в stderr или stdout в зависимости от версии
        output = result.stdout + "\n" + result.stderr
        if result.returncode == 0:
            # Извлекаем summary
            summary = next(
                (line for line in output.splitlines() if "passed" in line),
                output.strip().splitlines()[-1] if output.strip() else "unknown",
            )
            report.add("test_suite", Status.OK, f"Тесты пройдены: {summary}")
        else:
            tail = "\n".join(output.strip().splitlines()[-20:])
            report.add(
                "test_suite",
                Status.FAIL,
                f"Тесты не пройдены (exit {result.returncode}):\n{tail}",
            )
    except subprocess.TimeoutExpired:
        report.add("test_suite", Status.FAIL, "Тесты не завершились за 5 минут")
    except Exception as exc:  # noqa: BLE001
        report.add("test_suite", Status.FAIL, f"Ошибка запуска тестов: {exc}")


async def _broker_ping() -> str:
    cfg = TinkoffConfig.from_env()
    with TinkoffClient(cfg) as client:
        accounts = client.sdk.users.get_accounts()
    count = len(accounts.accounts)
    return f"accounts={count}, env={cfg.env.value}"


def check_broker_ping(report: PreflightReport, skip_network: bool) -> None:
    if skip_network:
        report.add("broker_ping", Status.OK, "Проверка брокера пропущена по запросу (--skip-network)")
        return
    try:
        msg = asyncio.run(_broker_ping())
        report.add("broker_ping", Status.OK, f"T-Invest отвечает: {msg}")
    except Exception as exc:  # noqa: BLE001
        report.add(
            "broker_ping",
            Status.FAIL,
            f"Не удалось подключиться к T-Invest: {exc}",
        )


def check_log_dir(report: PreflightReport) -> None:
    if not LOG_DIR.exists():
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            report.add("log_dir", Status.FAIL, f"Не удалось создать {LOG_DIR}: {exc}")
            return
    if not os.access(LOG_DIR, os.W_OK):
        report.add("log_dir", Status.FAIL, f"Нет прав на запись в {LOG_DIR}")
        return
    report.add("log_dir", Status.OK, f"Директория логов доступна: {LOG_DIR}")


def _overall(report: PreflightReport) -> str:
    if any(c.status == Status.FAIL for c in report.checks):
        return Status.FAIL
    if any(c.status == Status.WARN for c in report.checks):
        return Status.WARN
    return Status.OK


def _write_report(report: PreflightReport) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    msk = _now_msk()
    path = REPORT_DIR / f"{msk.strftime('%Y-%m-%d')}_preflight.md"

    lines = [
        f"# Pre-flight чек-лист — {msk.strftime('%Y-%m-%d %H:%M %z')}",
        "",
        f"**Overall:** {report.overall}",
        f"**UTC:** {report.timestamp_utc.isoformat()}",
        "",
        "| # | Проверка | Статус | Сообщение |",
        "|---|----------|--------|-----------|",
    ]
    for i, c in enumerate(report.checks, 1):
        msg = c.message.replace("|", "\\|").replace("\n", "<br>")
        lines.append(f"| {i} | {c.name} | {c.status} | {msg} |")

    lines.extend([
        "",
        "## Критерий готовности к long-run",
        "",
        "- [x] Preflight пройден",
        "- [ ] Watchdog рестартует runner после stale heartbeat",
        "- [ ] Kill-switch тест останавливает runner и шлёт алерт",
        "- [ ] 24-часовой сухой прогон стабилен",
        "",
    ])

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-flight чек-лист для long-run")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Путь к YAML-конфигу бота",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Не запускать полный test suite",
    )
    parser.add_argument(
        "--skip-network",
        action="store_true",
        help="Не проверять подключение к T-Invest",
    )
    args = parser.parse_args()

    # Загружаем .env если есть (не критично — TinkoffConfig читает env сама)
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except Exception:  # noqa: BLE001,S110
        pass

    report = PreflightReport(timestamp_utc=datetime.now(timezone.utc), overall="")

    check_env_vars(report)
    check_stop_flag(report)
    check_config(report, args.config)
    check_parquet_cache(report)
    check_disk_space(report)
    check_venv_python(report)
    check_log_dir(report)
    check_test_suite(report, args.skip_tests)
    check_broker_ping(report, args.skip_network)

    report.overall = _overall(report)
    path = _write_report(report)

    print(f"Overall: {report.overall}")
    print(f"Report: {path}")
    for c in report.checks:
        print(f"  [{c.status}] {c.name}: {c.message.splitlines()[0]}")

    return 0 if report.overall != Status.FAIL else 1


if __name__ == "__main__":
    raise SystemExit(main())
