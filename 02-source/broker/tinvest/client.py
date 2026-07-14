"""Обёртка над gRPC-каналом T-Invest с реконнектом и keep-alive.

Ответственность:
- Установить gRPC-канал к sandbox/prod-эндпоинту
- Поддерживать heartbeat (HTTP/2 ping)
- Переустанавливать канал при разрыве с экспоненциальным backoff
- Предоставлять единый интерфейс жизненного цикла: `connect / close / context manager`

Что НЕ делает этот модуль:
- Не выставляет заявки и не подписывается на стримы — это ответственность
  `orders.py` и `market_data.py`. Они используют `_channel` отсюда.
- Не знает про стратегии и риск — это ответственность верхних слоёв.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, Optional

import grpc
from tinkoff.invest import Client as TinkoffSDKClient
from tinkoff.invest.constants import (
    INVEST_GRPC_API,
    INVEST_GRPC_API_SANDBOX,
)

from core.clock import SystemClock

log = logging.getLogger(__name__)


class TInvestEnv(str, Enum):
    SANDBOX = "sandbox"
    PROD = "prod"


# Длительности
DEFAULT_KEEPALIVE_TIME_MS = 30_000  # 30 с — пинг раз в 30 с
DEFAULT_KEEPALIVE_TIMEOUT_MS = 10_000  # 10 с на ответ
DEFAULT_RECONNECT_INITIAL_DELAY = 0.5
DEFAULT_RECONNECT_MAX_DELAY = 30.0
DEFAULT_RECONNECT_MAX_ATTEMPTS = 10


@dataclass
class TinkoffConfig:
    """Конфигурация подключения. Читается из .env."""

    token: str
    env: TInvestEnv = TInvestEnv.SANDBOX

    keepalive_time_ms: int = DEFAULT_KEEPALIVE_TIME_MS
    keepalive_timeout_ms: int = DEFAULT_KEEPALIVE_TIMEOUT_MS

    reconnect_initial_delay: float = DEFAULT_RECONNECT_INITIAL_DELAY
    reconnect_max_delay: float = DEFAULT_RECONNECT_MAX_DELAY
    reconnect_max_attempts: int = DEFAULT_RECONNECT_MAX_ATTEMPTS

    @classmethod
    def from_env(cls) -> "TinkoffConfig":
        token = os.getenv("T_INVEST_SANDBOX_TOKEN") or os.getenv(
            "T_INVEST_PROD_TOKEN"
        )
        if not token:
            raise ValueError(
                "Не задан ни T_INVEST_SANDBOX_TOKEN, ни T_INVEST_PROD_TOKEN"
            )
        # По умолчанию — sandbox. Для prod выставить TRADING_MODE=live и
        # положить T_INVEST_PROD_TOKEN.
        env = TInvestEnv.SANDBOX
        if os.getenv("TRADING_MODE", "paper").lower() == "live":
            env = TInvestEnv.PROD
        return cls(token=token, env=env)

    @property
    def target(self) -> str:
        return (
            INVEST_GRPC_API_SANDBOX
            if self.env == TInvestEnv.SANDBOX
            else INVEST_GRPC_API
        )


class TinkoffClient:
    """Управляет gRPC-каналом и жизненным циклом соединения.

    Использование:
        cfg = TinkoffConfig.from_env()
        with TinkoffClient(cfg) as client:
            sdk = client.sdk
            accounts = sdk.users.get_accounts()

    Потокобезопасность: реконнект сериализован локом. `sdk` (tinkoff.invest.Client)
    нельзя держать между реконнектами — после reconnect() старый объект
    становится невалидным. Внутренние сервисы (market_data, orders) должны
    пересоздавать свои подписки.
    """

    def __init__(self, config: TinkoffConfig) -> None:
        self._config = config
        self._channel: Optional[grpc.Channel] = None
        self._sdk: Optional[TinkoffSDKClient] = None  # Client — владеет каналом
        self._services: Optional[object] = None  # Services — для вызовов
        self._lock = threading.RLock()
        self._closed = False
        self._clock = SystemClock()

    # --- Контекстный менеджер ----------------------------------------------

    def __enter__(self) -> "TinkoffClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @contextmanager
    def safe(self) -> Iterator["TinkoffClient"]:
        """Context manager, который при исключении пытается реконнект перед close."""
        try:
            yield self
        except grpc.RpcError as e:
            log.warning("gRPC error in safe-context: %s — пробую реконнект", e)
            self.reconnect()
            raise
        finally:
            self.close()

    # --- Публичный API -----------------------------------------------------

    @property
    def sdk(self) -> object:
        """Services-объект для вызовов gRPC-методов (users, market_data, ...).

        Это то, что возвращает `TinkoffSDKClient.__enter__()` — Services,
        у которого уже инициализированы все стабы (users.get_accounts и т.п.).

        Raises:
            RuntimeError: если клиент не подключён или уже закрыт
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("TinkoffClient уже закрыт")
            if self._services is None:
                raise RuntimeError(
                    "TinkoffClient не подключён — вызовите connect() "
                    "или используйте контекстный менеджер"
                )
            return self._services

    @property
    def config(self) -> TinkoffConfig:
        return self._config

    def is_connected(self) -> bool:
        with self._lock:
            return self._channel is not None and not self._closed

    def connect(self) -> None:
        """Установить gRPC-канал. Идемпотентно."""
        with self._lock:
            if self._channel is not None:
                return
            self._open_channel_locked()

    def close(self) -> None:
        """Закрыть канал. Идемпотентно."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._sdk is not None:
                try:
                    self._sdk.__exit__(None, None, None)
                except Exception as e:  # noqa: BLE001
                    log.warning("Ошибка при закрытии SDK: %s", e)
                self._sdk = None
                self._services = None
            self._channel = None  # SDK закрыл канал сам
            log.info("TinkoffClient закрыт (env=%s)", self._config.env)

    def reconnect(self) -> None:
        """Переустановить канал с экспоненциальным backoff.

        Используется при:
        - разрыве соединения (обнаружен стримом или руками)
        - изменении токена
        - переключении sandbox ↔ prod
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Нельзя реконнектить закрытый клиент")
            log.warning("Реконнект T-Invest (env=%s)...", self._config.env)
            # Закрываем старое
            if self._sdk is not None:
                try:
                    self._sdk.__exit__(None, None, None)
                except Exception:  # noqa: BLE001
                    pass
                self._sdk = None
                self._services = None
            self._channel = None  # SDK закрыл канал сам

            delay = self._config.reconnect_initial_delay
            for attempt in range(1, self._config.reconnect_max_attempts + 1):
                try:
                    self._open_channel_locked()
                    log.info(
                        "Реконнект успешен с попытки %d (env=%s)",
                        attempt,
                        self._config.env,
                    )
                    return
                except grpc.RpcError as e:
                    log.warning(
                        "Реконнект: попытка %d/%d не удалась: %s. "
                        "Жду %.1f с",
                        attempt,
                        self._config.reconnect_max_attempts,
                        e,
                        delay,
                    )
                    if attempt == self._config.reconnect_max_attempts:
                        raise ConnectionError(
                            f"Не удалось реконнектиться к T-Invest "
                            f"за {self._config.reconnect_max_attempts} попыток"
                        ) from e
                    time.sleep(delay)
                    delay = min(delay * 2, self._config.reconnect_max_delay)

    # --- Внутренние методы (вызываются под _lock) --------------------------

    def _open_channel_locked(self) -> None:
        """Создать новый gRPC-канал и обёртку SDK. Без блокировки.

        SDK `tinkoff.invest.Client` сам управляет каналом, мы только
        передаём ему options для keep-alive и target. Канал живёт
        внутри SDK, а снаружи управляем lifecycle через __enter__/__exit__.
        """
        keepalive_opts = [
            ("grpc.keepalive_time_ms", self._config.keepalive_time_ms),
            ("grpc.keepalive_timeout_ms", self._config.keepalive_timeout_ms),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        # Создаём Client (владеет каналом) и входим в контекст.
        # __enter__ возвращает Services, у которого есть users/market_data/...
        self._sdk = TinkoffSDKClient(
            self._config.token,
            target=self._config.target,
            options=keepalive_opts,
        )
        self._services = self._sdk.__enter__()
        # Проверяем готовность канала по первому лёгкому вызову.
        try:
            self._services.users.get_accounts()  # type: ignore[attr-defined]
        except Exception as e:  # noqa: BLE001
            self._sdk.__exit__(None, None, None)
            self._sdk = None
            self._services = None
            raise ConnectionError(
                f"Не удалось установить связь с T-Invest: {e}"
            ) from e
        self._channel = self._sdk._channel  # type: ignore[attr-defined]
        log.info(
            "TinkoffClient подключён к %s (env=%s, token=...%s)",
            self._config.target,
            self._config.env,
            self._config.token[-6:],
        )
