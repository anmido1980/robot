"""Ротация логов и SQLite-журнала.

- Удаляет старые .log-файлы и их .zip-архивы в 06-logs/runs (retention_days).
- Архивирует trades.db, если он превышает max_db_size_mb, и создаёт новый.
- Делает VACUUM для текущей БД (не чаще once_per_days).

Использование:
    python 03-scripts/rotate_logs.py [--log-dir 06-logs/runs] [--db 06-logs/journal/donchian_v2.sqlite]
"""

from __future__ import annotations

import argparse
import gzip
import logging
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


class RotationConfig:
    """Конфигурация ротации."""

    def __init__(
        self,
        log_dir: Path,
        journal_db: Path,
        retention_days: int = 14,
        max_db_size_mb: float = 500.0,
        vacuum_days: int = 1,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.journal_db = Path(journal_db)
        self.retention_days = retention_days
        self.max_db_size_mb = max_db_size_mb
        self.vacuum_days = vacuum_days


class LogRotator:
    """Ротация файлов логов и SQLite-журнала."""

    def __init__(self, config: RotationConfig) -> None:
        self.config = config

    # --- Публичный API -------------------------------------------------------

    def run(self) -> None:
        """Выполнить полный цикл ротации."""
        log.info("Запуск ротации")
        self._rotate_log_files()
        self._rotate_journal_db()
        self._vacuum_journal()
        log.info("Ротация завершена")

    # --- Лог-файлы -----------------------------------------------------------

    def _rotate_log_files(self) -> None:
        """Удалить лог-файлы старше retention_days (включая .zip)."""
        if not self.config.log_dir.exists():
            log.info("Директория логов не существует: %s", self.config.log_dir)
            return

        cutoff = datetime.now() - timedelta(days=self.config.retention_days)
        removed = 0
        for ext in ("*.log", "*.log.zip"):
            for path in self.config.log_dir.glob(ext):
                try:
                    mtime = datetime.fromtimestamp(path.stat().st_mtime)
                    if mtime < cutoff:
                        path.unlink()
                        removed += 1
                        log.info("Удалён старый лог: %s", path)
                except OSError as e:
                    log.warning("Не удалось удалить %s: %s", path, e)

        log.info("Удалено старых логов: %d", removed)

    # --- SQLite журнал -------------------------------------------------------

    def _rotate_journal_db(self) -> None:
        """Архивировать trades.db, если он превышает max_db_size_mb."""
        db = self.config.journal_db
        if not db.exists():
            log.info("Журнал не найден: %s", db)
            return

        size_mb = db.stat().st_size / (1024 * 1024)
        if size_mb <= self.config.max_db_size_mb:
            log.info("Размер журнала %.1f MB ≤ %.1f MB, архивация не нужна", size_mb, self.config.max_db_size_mb)
            return

        archive_name = db.with_suffix(f".db.{datetime.now():%Y%m%d_%H%M%S}.gz")
        try:
            with db.open("rb") as src, gzip.open(archive_name, "wb") as dst:
                shutil.copyfileobj(src, dst)
            db.unlink()
            db.touch()
            log.info("Журнал архивирован: %s → %s", db, archive_name)
        except OSError as e:
            log.error("Ошибка архивации журнала: %s", e)

    def _vacuum_journal(self) -> None:
        """Сделать VACUUM для журнала, если давно не делался."""
        import sqlite3

        db = self.config.journal_db
        if not db.exists():
            return

        marker = db.with_suffix(".last_vacuum")
        now = datetime.now()
        if marker.exists():
            try:
                last = datetime.fromisoformat(marker.read_text(encoding="utf-8").strip())
                if (now - last).days < self.config.vacuum_days:
                    log.debug("VACUUM не требуется")
                    return
            except Exception as e:  # noqa: BLE001
                log.warning("Не удалось прочитать last_vacuum: %s", e)

        try:
            conn = sqlite3.connect(str(db))
            conn.execute("VACUUM")
            conn.close()
            marker.write_text(now.isoformat(), encoding="utf-8")
            log.info("VACUUM выполнен для %s", db)
        except Exception as e:  # noqa: BLE001
            log.warning("Ошибка VACUUM: %s", e)


# --- Точка входа -----------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ротация логов и журнала")
    parser.add_argument("--log-dir", type=Path, default=Path("06-logs/runs"), help="Директория лог-файлов")
    parser.add_argument("--db", type=Path, default=Path("06-logs/journal/donchian_v2.sqlite"), help="Путь к SQLite журналу")
    parser.add_argument("--retention-days", type=int, default=14, help="Хранить логи N дней")
    parser.add_argument("--max-db-size-mb", type=float, default=500.0, help="Архивировать БД при размере > MB")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = RotationConfig(
        log_dir=args.log_dir,
        journal_db=args.db,
        retention_days=args.retention_days,
        max_db_size_mb=args.max_db_size_mb,
    )
    rotator = LogRotator(config)
    rotator.run()


if __name__ == "__main__":
    main()
