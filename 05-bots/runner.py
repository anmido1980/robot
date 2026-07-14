"""Paper-trading runner для Donchian v2 (Si/1h).

Главный цикл:
  1. setup()  — подключение к T-Invest sandbox, find_instruments, EventBus,
                RiskManager, TradeJournal, стратегия.
  2. _warmup() — загрузка истории (Parquet или T-Invest get_last_candles),
                 прогон через strategy.on_candle для прогрева индикаторов.
  3. run()    — async-цикл: stream_candles → clearing check → kill-switch
                 check → strategy.on_candle → _build_order → risk_manager.
                 Параллельно фоновая таска _order_state_publisher читает
                 stream_order_states, обогащает figi/side из локального
                 реестра и публикует OrderStateEvent в EventBus. TradeJournal
                 (подписан на "order_state") сам записывает fills в SQLite.

Конфиг: 05-bots/donchian_paper.yaml
Стратегия: 01-docs/strategies/donchian_v2.md
Отчёт smoke: 04-output/2026-06-15_runner_paper_smoke.md
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from loguru import logger

# Подключаем 02-source к sys.path (для запуска напрямую: `python 05-bots/runner.py`)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "02-source"))

# Загружаем .env перед импортом broker-конфигурации
load_dotenv(_PROJECT_ROOT / ".env")

# Зависимости из core и брокера
from broker.tinvest.market_data import CANDLE_INTERVAL_MAP  # noqa: E402
from core.alerts import TelegramAlerter  # noqa: E402
from core.clock import MOSCOW_TZ, Clock, SystemClock  # noqa: E402
from core.events import EventBus, OrderStateEvent, TradeEvent  # noqa: E402
from core.models import (  # noqa: E402
    Candle,
    Order,
    OrderType,
    Side,
    TimeInForce,
    Trade,
)


# --- Конфигурация runner'а --------------------------------------------------


@dataclass
class RunnerConfig:
    """Параметры paper-trading. Захардкожены; YAML-парсер — phase 4 long-run."""

    # Режим
    mode: str = "paper"  # paper | live

    # Инструмент
    ticker: str = "Si"
    class_code: str = "SPBFUT"
    figi: str = ""  # заполнится авто-поиском (find_instruments по ticker)
    account_id: str = ""  # заполнится авто-поиском (sandbox account)
    min_step: Decimal = Decimal("1")  # минимальный шаг цены

    # Стратегия
    strategy_id: str = "donchian_v2_paper"
    entry_period: int = 20
    exit_period: int = 10
    atr_period: int = 14
    stop_atr_mult: float = 2.0
    adx_min: float = 15.0
    adx_period: int = 14
    volume_min_ratio: float = 0.0
    ema_period: int = 0

    # Размер
    contracts: int = 1

    # Таймфрейм и история
    timeframe: str = "1h"
    history_candles: int = 60  # минимум для прогрева индикаторов
    # Кеш 1h для прогрева (ticker-уровень, склеен по контрактам). При None —
    # PaperTradingRunner подставит дефолт путь от project root.
    parquet_fallback: Optional[Path] = None

    # Клиринг и расписание
    clearing_start: time = time(23, 50)
    clearing_end: time = time(0, 30)
    work_days: set[int] = field(default_factory=lambda: {0, 1, 2, 3, 4})  # пн–пт

    # Пути
    log_dir: Path = field(default_factory=lambda: Path("06-logs/runs"))
    journal_db: Path = field(default_factory=lambda: Path("06-logs/journal/donchian_v2.sqlite"))
    daily_report_dir: Path = field(default_factory=lambda: Path("01-docs/journal"))

    # Лимит работы (для smoke)
    max_runtime_minutes: int = 0  # 0 = без лимита

    @classmethod
    def from_yaml(cls, path: Path) -> "RunnerConfig":
        """Загрузить RunnerConfig из YAML-файла (см. 05-bots/donchian_paper.yaml)."""
        import yaml

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        def _get(keys: list[str], default: Any = None) -> Any:
            node: Any = raw
            for key in keys:
                if not isinstance(node, dict):
                    return default
                node = node.get(key, default)
                if node is None:
                    return default
            return node

        def _parse_time(value: Any) -> time:
            if isinstance(value, time):
                return value
            if isinstance(value, str):
                h, m = value.split(":", 1)
                return time(int(h), int(m))
            raise ValueError(f"Невалидное время: {value!r}")

        def _parse_work_days(value: Any) -> set[int]:
            if value is None:
                return {0, 1, 2, 3, 4}
            names = {d.lower() for d in value}
            mapping = {
                "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4,
                "sat": 5, "sun": 6,
            }
            return {mapping[n] for n in names if n in mapping}

        cfg = cls()

        # Режим
        cfg.mode = _get(["mode"], cfg.mode)

        # Брокер / инструмент
        cfg.ticker = _get(["broker", "ticker"], cfg.ticker)
        cfg.class_code = _get(["broker", "class_code"], cfg.class_code)

        # Стратегия
        strategy_cfg = _get(["strategy", "config"], {})
        cfg.strategy_id = strategy_cfg.get("strategy_id", cfg.strategy_id)
        cfg.entry_period = strategy_cfg.get("entry_period", cfg.entry_period)
        cfg.exit_period = strategy_cfg.get("exit_period", cfg.exit_period)
        cfg.atr_period = strategy_cfg.get("atr_period", cfg.atr_period)
        cfg.stop_atr_mult = strategy_cfg.get("stop_atr_mult", cfg.stop_atr_mult)
        cfg.adx_min = strategy_cfg.get("adx_min", cfg.adx_min)
        cfg.adx_period = strategy_cfg.get("adx_period", cfg.adx_period)
        cfg.volume_min_ratio = strategy_cfg.get(
            "volume_min_ratio", cfg.volume_min_ratio
        )
        cfg.ema_period = strategy_cfg.get("ema_period", cfg.ema_period)
        cfg.timeframe = _get(["strategy", "timeframe"], cfg.timeframe)

        # Позиция
        cfg.contracts = _get(["position", "contracts"], cfg.contracts)

        # История
        cfg.history_candles = _get(
            ["market_data", "history_days"], cfg.history_candles
        )

        # Клиринг / расписание
        cfg.clearing_start = _parse_time(
            _get(["schedule", "clearing_start"], cfg.clearing_start)
        )
        cfg.clearing_end = _parse_time(
            _get(["schedule", "clearing_end"], cfg.clearing_end)
        )
        cfg.work_days = _parse_work_days(_get(["schedule", "work_days"]))

        # Пути
        log_dir = _get(["logging", "log_dir"])
        if log_dir:
            cfg.log_dir = Path(log_dir)
        journal_path = _get(["logging", "journal", "path"])
        if journal_path:
            cfg.journal_db = Path(journal_path)
        daily_report_dir = _get(["logging", "daily_report", "output_dir"])
        if daily_report_dir:
            cfg.daily_report_dir = Path(daily_report_dir)

        return cfg


# --- Локальный реестр ордеров для обогащения OrderStateEvent ----------------


@dataclass
class _OrderRegistry:
    """Хранит order_request_id → Order для обогащения OrderStateEvent.

    RiskManager.post_order() возвращает OrderStateInfo без figi и side —
    они нужны TradeJournal для записи fills. Здесь держим маппинг,
    чтобы _order_state_publisher мог подставить недостающие поля.
    """

    by_request_id: dict[str, Order] = field(default_factory=dict)
    _seen_trade_ids: set[str] = field(default_factory=set)

    def remember(self, order: Order) -> None:
        self.by_request_id[order.order_request_id] = order

    def get(self, order_request_id: str) -> Optional[Order]:
        return self.by_request_id.get(order_request_id)

    def is_new_fill(self, state: OrderStateInfo) -> bool:
        key = f"{state.order_request_id}-{state.timestamp.isoformat()}"
        if key in self._seen_trade_ids:
            return False
        self._seen_trade_ids.add(key)
        return True


# --- Основной класс --------------------------------------------------------


class PaperTradingRunner:
    """Paper-trading runner. Конструируется из RunnerConfig, запускается .run()."""

    def __init__(self, config: Optional[RunnerConfig] = None) -> None:
        self.config = config or RunnerConfig()
        # Дефолтный кеш 1h для прогрева (если не задан явно)
        if self.config.parquet_fallback is None:
            self.config.parquet_fallback = (
                _PROJECT_ROOT / "06-logs" / "candles" / "Si_swing_1h.parquet"
            )
        self._registry = _OrderRegistry()
        self._stopped = False
        self._stats = {
            "candles_received": 0,
            "signals_generated": 0,
            "orders_posted": 0,
            "orders_filled": 0,
            "orders_rejected": 0,
            "kill_switch_breaks": 0,
            "exceptions": 0,
        }

        # Эти атрибуты инициализируются в setup()
        self.client: Any = None
        self.market_data: Any = None
        self.orders: Any = None
        self.portfolio: Any = None
        self.risk_manager: Any = None
        self.clock: Clock = SystemClock()
        self.event_bus: EventBus = EventBus()
        self.strategy: Any = None
        self.journal: Any = None
        self.pnl: Any = None
        self.pnl_subscriber_id: Any = None
        self.daily_report: Any = None
        self.alerter: TelegramAlerter = TelegramAlerter()
        self.initial_capital: Decimal = Decimal("1000000")

        # Последняя обработанная свеча — для heartbeat
        self._last_candle: Optional[Candle] = None

        # Сохраняем ссылки на пути — нужны в setup()
        self.log_dir = self.config.log_dir
        self.journal_db = self.config.journal_db

        # Лог-файл настраивается в setup()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            self.log_dir / f"runner_{datetime.now(MOSCOW_TZ):%Y-%m-%d_%H-%M-%S}.log",
            rotation="10 MB",
            level="INFO",
        )

    # --- setup() -------------------------------------------------------------

    async def setup(self) -> None:
        """Подключение к T-Invest sandbox + инициализация всех модулей."""
        from broker.tinvest.client import TinkoffClient, TinkoffConfig
        from broker.tinvest.market_data import TinkoffMarketData
        from broker.tinvest.orders import TinkoffOrders
        from broker.tinvest.portfolio import TinkoffPortfolio
        from journal.pnl import PnLCalculator
        from journal.report import DailyReport
        from journal.trades import TradeJournal
        from risk.config import RiskConfig
        from risk.manager import RiskManager
        from strategies.swing.donchian_breakout_v2.strategy import (
            DonchianBreakoutV2,
            DonchianConfigV2,
        )

        cfg = TinkoffConfig.from_env()
        self.client = TinkoffClient(cfg)
        self.client.connect()

        # Открыть sandbox-счёт, если нужно
        try:
            accounts = self.client.sdk.users.get_accounts().accounts
            if not accounts and cfg.env.value == "sandbox":
                logger.info("Sandbox-счёт отсутствует — открываю через open_sandbox_account")
                try:
                    self.client.sdk.sandbox.open_sandbox_account()
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"open_sandbox_account: {e} — продолжаю без счёта")
                    accounts = self.client.sdk.users.get_accounts().accounts
        except Exception as e:  # noqa: BLE001
            logger.warning(f"get_accounts: {e} — продолжаю с пустым списком")
            accounts = []

        # Выбор счёта: первый из accounts
        self.config.account_id = accounts[0].id if accounts else "sandbox-default"
        logger.info(
            f"Runner настроен: env={cfg.env.value}, account_id={self.config.account_id}"
        )

        # Найти ближайший фьючерс Si по экспирации
        # Используем TinkoffPortfolio.find_instruments (корректно маппит
        # min_step), а не TinkoffMarketData.find_instruments (баг: Decimal(""))
        instruments = []
        try:
            portfolio_temp = TinkoffPortfolio(self.client)
            instruments = await portfolio_temp.find_instruments(
                ticker=self.config.ticker
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"find_instruments: {e}")

        if instruments:
            # Получаем сырые SDK-объекты для сортировки по expiration_date
            try:
                sdk_futures = self.client.sdk.instruments.futures().instruments
                exp_map: dict[str, str] = {
                    f.figi: getattr(f, "expiration_date", "9999-12-31")
                    for f in sdk_futures
                }
            except Exception:
                exp_map = {}

            def _exp_key(ins: Any) -> str:
                # expiration_date — у SDK-объекта; core.Instrument не хранит
                return exp_map.get(ins.figi, ins.figi)

            instruments.sort(key=_exp_key)
            nearest = instruments[0]
            self.config.figi = nearest.figi
            self.config.min_step = nearest.min_step or Decimal("1")
            logger.info(
                f"Ближайший контракт: ticker={nearest.ticker}, figi={nearest.figi}, "
                f"min_step={nearest.min_step}, exp={exp_map.get(nearest.figi, '?')}"
            )
        else:
            logger.warning(
                f"Инструменты по ticker={self.config.ticker} не найдены. "
                f"Использую figi из конфига (если задан): {self.config.figi!r}"
            )

        # Сервисы
        self.market_data = TinkoffMarketData(self.client)
        self.orders = TinkoffOrders(self.client)
        self.portfolio = TinkoffPortfolio(self.client)

        # Risk-менеджер
        self.risk_manager = RiskManager(
            gateway=self.orders,
            portfolio=self.portfolio,
            event_bus=self.event_bus,
            clock=self.clock,
            config=RiskConfig(),
        )

        # P&L + журнал + отчёт
        self.pnl = PnLCalculator(self.clock)
        self.pnl_subscriber_id = self.event_bus.subscribe("trade", self._on_trade_event)
        self.journal = TradeJournal(self.journal_db, event_bus=self.event_bus)
        self.journal.connect()
        self._restore_pnl_from_journal()
        self.daily_report = DailyReport(
            journal_dir=self.config.daily_report_dir
        )

        # Стратегия
        self.strategy = DonchianBreakoutV2(
            DonchianConfigV2(
                entry_period=self.config.entry_period,
                exit_period=self.config.exit_period,
                atr_period=self.config.atr_period,
                stop_atr_mult=self.config.stop_atr_mult,
                adx_min=self.config.adx_min,
                adx_period=self.config.adx_period,
                volume_min_ratio=self.config.volume_min_ratio,
                ema_period=self.config.ema_period,
                strategy_id=self.config.strategy_id,
            )
        )

        # Прогрев индикаторов
        if self.config.figi:
            await self._warmup(
                md=self.market_data,
                strategy=self.strategy,
                figi=self.config.figi,
                interval=self.config.timeframe,
                parquet_fallback=self.config.parquet_fallback,
            )
        else:
            logger.warning("figi пуст — пропускаю прогрев")

    def _restore_pnl_from_journal(self) -> None:
        """Восстановить P&L из журнала сделок при старте.

        Воспроизводим все сделки с начала текущего торгового дня
        (после клиринга). Сделки до клиринга вчерашнего дня уже учтены
        в realized_pnl total, их не пересчитываем.
        """
        if self.journal is None or self.pnl is None:
            return
        now = self.clock.now()
        # Начало текущего торгового дня: после клиринга 00:30
        since = now.replace(hour=0, minute=30, second=0, microsecond=0)
        if now.time() < since.time():
            # Если сейчас до 00:30 — значит ещё вчерашняя сессия
            since -= timedelta(days=1)
        try:
            trades = self.journal.get_trades_since(since)
            self.pnl.replay_trades(trades, since=since)
            logger.info(f"Восстановлено {len(trades)} сделок из журнала с {since}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Не удалось восстановить P&L из журнала: {e}")

    # --- _warmup() -----------------------------------------------------------

    async def _warmup(
        self,
        md: Any,
        strategy: Any,
        figi: str,
        interval: str,
        parquet_fallback: Optional[Path],
    ) -> int:
        """Загрузить историю и прогнать через strategy.on_candle.

        Приоритет:
        1. Parquet-файл (если задан и существует).
        2. Иначе — T-Invest get_last_candles(figi, interval, history_candles).

        Returns:
            Кол-во прогнанных свечей.
        """
        # 1. Parquet
        if parquet_fallback and parquet_fallback.exists():
            try:
                from backtest.data_loader import load_from_parquet

                candles = load_from_parquet(parquet_fallback, figi=figi)
                for c in candles:
                    strategy.on_candle(c)
                logger.info(
                    f"Прогрев: {len(candles)} свечей из Parquet {parquet_fallback}"
                )
                return len(candles)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"Ошибка загрузки Parquet {parquet_fallback}: {e} — "
                    f"фоллбэк на T-Invest"
                )

        # 2. T-Invest get_last_candles (fallback)
        # Известный баг SDK 0.2.0b59: HistoricCandle не имеет .figi —
        # TinkoffMarketData._to_candle падает с AttributeError.
        # Оборачиваем в try/except — если упало, прогрев пропускаем,
        # стратегия начнёт работать "с нуля" (первые сигналы — после entry_period свечей).
        try:
            await md.subscribe_candles(figi, interval)
            candles = await md.get_last_candles(
                figi=figi, interval=interval, count=self.config.history_candles
            )
            for c in candles:
                strategy.on_candle(c)
            logger.info(
                f"Прогрев: {len(candles)} свечей из T-Invest "
                f"(figi={figi}, interval={interval})"
            )
            return len(candles)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"T-Invest get_last_candles упал ({e}) — прогрев пропущен, "
                f"стратегия стартует без истории. "
                f"Рекомендуется подготовить Parquet-кеш."
            )
            return 0

    # --- _build_order() ------------------------------------------------------

    def _build_order(
        self,
        signal_side: Side,
        candle: Candle,
        is_exit: bool,
    ) -> Order:
        """Построить Order из сигнала.

        Вход (is_exit=False): LIMIT по close ± 1 tick (чтобы исполнилось).
        Выход (is_exit=True): MARKET, цена None — гарантируем исполнение.
        """
        order_id = str(uuid.uuid4())
        tick = self.config.min_step

        if is_exit:
            order_type = OrderType.MARKET
            price = None
        else:
            order_type = OrderType.LIMIT
            base = Decimal(str(candle.close))
            if signal_side == Side.BUY:
                price = base + tick  # агрессивно для исполнения
            else:
                price = base - tick

        comment = (
            f"runner|figi={self.config.figi or candle.figi}|"
            f"side={signal_side.value}|"
            f"{'exit' if is_exit else 'entry'}"
        )
        return Order(
            order_request_id=order_id,
            account_id=self.config.account_id,
            figi=self.config.figi or candle.figi,
            side=signal_side,
            order_type=order_type,
            quantity=self.config.contracts,
            price=price,
            time_in_force=TimeInForce.DAY,
            strategy_id=self.config.strategy_id,
            comment=comment,
        )

    # --- _is_clearing() ------------------------------------------------------

    def _is_clearing(self, now: datetime) -> bool:
        """Клиринг: [clearing_start, midnight) ∪ [00:00, clearing_end)."""
        t = now.time()
        return t >= self.config.clearing_start or t < self.config.clearing_end

    # --- _is_workday() -------------------------------------------------------

    def _is_workday(self, now: datetime) -> bool:
        """True, если now.weekday() входит в разрешённые рабочие дни."""
        return now.weekday() in self.config.work_days

    # --- _on_trade_event ----------------------------------------------------

    async def _on_trade_event(self, event: TradeEvent) -> None:
        """Обновить PnLCalculator по сделке из TradeEvent."""
        trade = event.payload
        if trade is None:
            return
        self.pnl.update_fill(trade)
        logger.debug(
            f"PnL updated: {trade.side.value} {trade.figi} x{trade.quantity} @ {trade.price}"
        )

    # --- _order_state_publisher() -------------------------------------------

    async def _order_state_publisher(self) -> None:
        """Фоновая таска: stream_order_states → обогатить figi/side → publish.

        T-Invest stream_order_states не публикует в EventBus сам — берём
        из risk_manager (проксирует TinkoffOrders.stream_order_states) и
        публикуем вручную. TradeJournal подписан на "order_state" и
        автоматически записывает FILLED-сделки.
        """
        try:
            async for state_info in self.risk_manager.stream_order_states():
                if self._stopped:
                    return

                # Обогащаем account/comment из реестра
                order = self._registry.get(state_info.order_request_id)
                if order is not None:
                    if not state_info.account_id:
                        state_info.account_id = order.account_id
                    state_info.comment = order.comment

                # Учёт статистики
                if state_info.state.value == "filled":
                    self._stats["orders_filled"] += 1
                elif state_info.state.value == "rejected":
                    self._stats["orders_rejected"] += 1

                await self.event_bus.publish(OrderStateEvent(state_info))

                # Публикация TradeEvent для PnLCalculator (если fill)
                if (
                    state_info.state.value in ("filled", "partially_filled")
                    and state_info.average_price is not None
                    and state_info.filled_quantity > 0
                    and self._registry.is_new_fill(state_info)
                ):
                    trade_figi = order.figi if order is not None else state_info.figi
                    trade_side = order.side if order is not None else state_info.side
                    trade = Trade(
                        trade_id=f"fill-{state_info.order_request_id}-{state_info.timestamp.isoformat()}",
                        order_request_id=state_info.order_request_id,
                        account_id=state_info.account_id or (order.account_id if order is not None else ""),
                        figi=trade_figi or "",
                        side=trade_side,
                        quantity=state_info.filled_quantity,
                        price=state_info.average_price,
                        timestamp=state_info.timestamp,
                    )
                    await self.event_bus.publish(TradeEvent(trade))
        except asyncio.CancelledError:
            logger.info("OrderStatePublisher: остановлен")
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception(f"OrderStatePublisher упал: {e}")
            self._stats["exceptions"] += 1

    # --- run() ---------------------------------------------------------------

    async def _heartbeat_loop(self, interval_seconds: int = 60) -> None:
        """Фоновая таска: писать heartbeat.json каждые interval_seconds."""
        heartbeat_path = self.log_dir.parent / "heartbeat.json"
        while not self._stopped:
            try:
                await self._write_heartbeat(heartbeat_path)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Ошибка записи heartbeat: {e}")
            await asyncio.sleep(float(interval_seconds))

    async def _write_heartbeat(self, path: Path) -> None:
        """Записать текущий heartbeat в JSON."""
        import json

        now = self.clock.now()
        total_pnl = self.pnl.total_pnl() if self.pnl is not None else Decimal(0)
        unrealized_pnl = (
            self.pnl.unrealized_pnl() if self.pnl is not None else Decimal(0)
        )
        daily_pnl = (
            self.pnl.daily_realized_pnl() + unrealized_pnl
            if self.pnl is not None
            else Decimal(0)
        )

        position_qty = 0
        last_price: Optional[float] = None
        last_candle_time: Optional[str] = None
        if self._last_candle is not None:
            last_candle_time = self._last_candle.timestamp.isoformat()
            last_price = float(self._last_candle.close)
        if self.pnl is not None:
            pos = self.pnl.get_position(self.config.figi)
            if pos is not None:
                position_qty = pos.quantity
                if pos.current_price is not None and last_price is None:
                    last_price = float(pos.current_price)

        cash = 0.0
        active_orders = 0
        try:
            if self.portfolio is not None and self.config.account_id:
                snapshot = await self.portfolio.get_portfolio(self.config.account_id)
                if snapshot.available_cash is not None:
                    cash = float(snapshot.available_cash.value)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"heartbeat: не удалось получить cash: {e}")

        status = "killed" if self.risk_manager.kill_switch.is_active() else "running"
        if self._stopped:
            status = "stopped"

        data = {
            "timestamp": now.isoformat(),
            "status": status,
            "last_candle_time": last_candle_time,
            "last_price": last_price,
            "daily_pnl": float(daily_pnl),
            "total_pnl": float(total_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "position_qty": position_qty,
            "cash": cash,
            "active_orders": active_orders,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    async def run(self) -> None:
        """Главный цикл paper-trading. Запускать через asyncio.run()."""
        if not self.client:
            await self.setup()

        logger.info(
            f"Runner запущен: ticker={self.config.ticker}, figi={self.config.figi}, "
            f"TF={self.config.timeframe}, mode={self.config.mode}"
        )

        # Лимит по времени (для smoke)
        deadline = None
        if self.config.max_runtime_minutes > 0:
            from datetime import timedelta

            deadline = self.clock.now() + timedelta(
                minutes=self.config.max_runtime_minutes
            )
            logger.info(
                f"Smoke: runner остановится через "
                f"{self.config.max_runtime_minutes} мин (≈{deadline:%H:%M:%S})"
            )

        # Фоновые задачи
        publisher_task = asyncio.create_task(self._order_state_publisher())
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval_seconds=60))

        # Polling-цикл: опрашиваем get_candles раз в 1 сек.
        # Прямой вызов — потому что TinkoffMarketData.stream_candles()
        # использует async-generator без таймаута и может "зависнуть"
        # в ожидании первой свечи (для 1h-ТФ между часами — это 1 час).
        from datetime import timedelta as _td

        from broker.tinvest.converters import quotation_to_decimal

        sdk = self.client.sdk
        ci, _ = CANDLE_INTERVAL_MAP[self.config.timeframe]
        seen: set = set()

        try:
            while not self._stopped:
                # Проверка deadline / kill-switch
                now = self.clock.now()
                if deadline is not None and now >= deadline:
                    logger.info(
                        f"Smoke: достигнут лимит {self.config.max_runtime_minutes} мин"
                    )
                    self._stopped = True
                    break

                # Polling
                try:
                    resp = sdk.market_data.get_candles(
                        figi=self.config.figi,
                        from_=self.clock.now() - _td(minutes=5),
                        to=self.clock.now(),
                        interval=ci,
                    )
                    for raw in resp.candles:
                        # HistoricCandle в SDK 0.2.0b59: raw.time —
                        # это Python datetime, а не protobuf Timestamp.
                        # figi отсутствует — передаём явно.
                        ts = raw.time
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=MOSCOW_TZ)
                        candle = Candle(
                            figi=self.config.figi,
                            timestamp=ts,
                            open=quotation_to_decimal(raw.open),
                            high=quotation_to_decimal(raw.high),
                            low=quotation_to_decimal(raw.low),
                            close=quotation_to_decimal(raw.close),
                            volume=int(raw.volume),
                        )
                        ts = candle.timestamp
                        key = (candle.figi, ts)
                        if key in seen:
                            continue
                        seen.add(key)
                        # Обрабатываем свечу через основной pipeline
                        stop = await self._process_candle(candle)
                        if stop:
                            self._stopped = True
                            break
                    if self._stopped:
                        break
                except Exception as e:  # noqa: BLE001
                    self._stats["exceptions"] += 1
                    logger.warning(f"get_candles error: {e}")

                await asyncio.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Остановка по Ctrl-C")
        finally:
            self._stopped = True
            publisher_task.cancel()
            heartbeat_task.cancel()
            try:
                await publisher_task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            await self.shutdown()
            self._print_summary()

    async def _process_candle(self, candle: Candle) -> bool:
        """Обработать одну свечу через pipeline. True = пора остановиться."""
        self._last_candle = candle
        now = self.clock.now()

        # Сброс дневного P&L при клиринге
        self.pnl.check_and_reset_daily()

        # Клиринг
        if self._is_clearing(now):
            return False

        # Обновить нереализованный P&L по текущей цене закрытия
        self.pnl.update_market_price(candle.figi, Decimal(str(candle.close)))

        # Обновить дневной P&L в RiskManager и проверить авто kill-switch
        total_pnl = self.pnl.total_pnl()
        self.risk_manager.update_daily_pnl(total_pnl)
        self.risk_manager.kill_switch.auto_activate_if_needed(
            current_value=self.initial_capital + total_pnl,
            start_value=self.initial_capital,
        )

        # Kill-switch
        if self.risk_manager.kill_switch.is_active():
            logger.warning("Kill-switch активен, останавливаемся")
            self._stats["kill_switch_breaks"] += 1
            try:
                await self.alerter.alert_kill_switch(
                    reason=self.risk_manager.kill_switch._reason or "unknown",
                    pnl=total_pnl,
                    capital=self.initial_capital,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Ошибка отправки kill-switch alert: {e}")
            return True

        # Выходной
        if not self._is_workday(now):
            return False

        # Свеча → стратегия
        self._stats["candles_received"] += 1
        signal = self.strategy.on_candle(candle)
        if signal is None:
            return False

        self._stats["signals_generated"] += 1

        # Сигнал: определить — вход или выход
        in_position = self.strategy._state.in_position
        last_side = self.strategy._state.position_side
        is_exit = in_position and (
            (last_side == Side.BUY and signal.side == Side.SELL)
            or (last_side == Side.SELL and signal.side == Side.BUY)
        )

        order = self._build_order(
            signal_side=signal.side,
            candle=candle,
            is_exit=is_exit,
        )
        self._registry.remember(order)

        # Отправляем через RiskManager
        from broker.tinvest.orders import (  # noqa: WPS433
            OrderExchangeUnavailable,
            OrderRejected,
        )
        from risk.manager import RiskViolation

        try:
            result = await self.risk_manager.post_order(order)
            self._stats["orders_posted"] += 1
            logger.info(
                f"Order: {signal.reason} {order.side.value} "
                f"{order.figi} qty={order.quantity} "
                f"type={order.order_type.value} → {result.state.value}"
            )
        except RiskViolation as e:
            logger.warning(f"Риск-отказ: {e.reason}")
        except (OrderRejected, OrderExchangeUnavailable) as e:
            logger.warning(f"Брокер отклонил: {e}")
        except Exception as e:  # noqa: BLE001
            self._stats["exceptions"] += 1
            logger.exception(f"Ошибка post_order: {e}")

        return False

    # --- shutdown() ----------------------------------------------------------

    async def shutdown(self) -> None:
        """Закрыть ресурсы: journal, TinkoffClient, сгенерировать отчёт."""
        # Ежедневный отчёт
        try:
            if self.daily_report is not None and self.journal is not None:
                report_path = self.daily_report.generate(
                    report_date=self.clock.now().date(),
                    journal=self.journal,
                    pnl=self.pnl,
                )
                logger.info(f"Daily report: {report_path}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Ошибка генерации daily report: {e}")

        try:
            if self.journal is not None:
                self.journal.close()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Ошибка закрытия journal: {e}")

        try:
            if self.client is not None:
                self.client.close()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Ошибка закрытия TinkoffClient: {e}")

    # --- Итоги ---------------------------------------------------------------

    def _print_summary(self) -> None:
        """Напечатать сводку по сессии в лог и stdout."""
        s = self._stats
        summary = (
            f"\n=== Сводка paper-trading ===\n"
            f"  Свечей получено:     {s['candles_received']}\n"
            f"  Сигналов:            {s['signals_generated']}\n"
            f"  Заявок подано:       {s['orders_posted']}\n"
            f"  Заявок исполнено:    {s['orders_filled']}\n"
            f"  Заявок отклонено:    {s['orders_rejected']}\n"
            f"  Kill-switch стопов:  {s['kill_switch_breaks']}\n"
            f"  Исключений в цикле:  {s['exceptions']}\n"
            f"==========================="
        )
        logger.info(summary)
        print(summary)


# --- Точка входа -----------------------------------------------------------


def main() -> None:
    """Запуск paper-trading с дефолтным конфигом."""
    config = RunnerConfig()
    asyncio.run(PaperTradingRunner(config).run())


if __name__ == "__main__":
    main()
