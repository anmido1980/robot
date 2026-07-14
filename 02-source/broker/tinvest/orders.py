"""Адаптер заявок T-Invest.

Реализует `OrderGateway` Protocol из `core.interfaces`:
- post_order — выставление заявки (идемпотентность через order_request_id)
- cancel_order — отмена заявки
- stream_order_states — polling статусов заявок

Исключения:
- OrderRejected — биржа/брокер отклонила заявку
- OrderNotFoundError — заявка не найдена при отмене/запросе статуса
- OrderValidationError — некорректные параметры заявки
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Optional

import grpc
from tinkoff.invest import OrderDirection, OrderType
from tinkoff.invest.exceptions import RequestError
from tinkoff.invest.schemas import (
    OrderExecutionReportStatus,
    Quotation,
)

from core.clock import SystemClock
from core.models import (
    Order,
    OrderState,
    OrderStateInfo,
    Side,
)

from .client import TinkoffClient
from .converters import (
    execution_status_to_order_state,
    quotation_to_decimal,
    tinkoff_time,
)

log = logging.getLogger(__name__)


# --- Исключения --------------------------------------------------------------


class OrderError(Exception):
    """Базовое исключение для ошибок заявок."""

    def __init__(self, message: str, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code


class OrderRejected(OrderError):
    """Биржа/брокер отклонила заявку."""

    def __init__(self, message: str, reject_reason: Optional[str] = None) -> None:
        super().__init__(message, code="rejected")
        self.reject_reason = reject_reason


class OrderNotFoundError(OrderError):
    """Заявка не найдена."""

    def __init__(self, order_id: str) -> None:
        super().__init__(f"Заявка не найдена: {order_id}", code="not_found")
        self.order_id = order_id


class OrderValidationError(OrderError):
    """Некорректные параметры заявки."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="validation_error")


class OrderExchangeUnavailable(OrderError):
    """Биржа недоступна для торговли (песочница, клиринг и т.п.)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="exchange_unavailable")


# --- Маппинги ----------------------------------------------------------------

# Side → OrderDirection
_SIDE_TO_DIRECTION = {
    Side.BUY: OrderDirection.ORDER_DIRECTION_BUY,
    Side.SELL: OrderDirection.ORDER_DIRECTION_SELL,
}

# core.OrderType → tinkoff OrderType
_ORDER_TYPE_MAP = {
    "limit": OrderType.ORDER_TYPE_LIMIT,
    "market": OrderType.ORDER_TYPE_MARKET,
    "bestprice": OrderType.ORDER_TYPE_BESTPRICE,
}

# gRPC StatusCode → наше исключение (приоритетный маппинг)
_GRPC_STATUS_TO_EXCEPTION = {
    grpc.StatusCode.NOT_FOUND: OrderNotFoundError,
    grpc.StatusCode.INVALID_ARGUMENT: OrderValidationError,
    grpc.StatusCode.UNAVAILABLE: OrderExchangeUnavailable,
    grpc.StatusCode.FAILED_PRECONDITION: OrderExchangeUnavailable,
}


# --- Вспомогательные ---------------------------------------------------------


def _decimal_to_quotation(value: Decimal) -> Quotation:
    """Decimal → tinkoff Quotation (units + nano).

    T-Invest API: units и nano одного знака (оба >= 0 или оба <= 0).
    Quotation.__init__ нормализует: переносит переполнение nano в units.
    Поэтому для отрицательных чисел передаём оба компонента отрицательными,
    и Quotation.__init__ нормализует корректно:
      Quotation(units=-100, nano=-500_000_000) → (units=-101, nano=500_000_000)
      что представляет -101 + 0.5 = -100.5 ✓
    """
    sign = -1 if value < 0 else 1
    abs_val = abs(value)
    units = int(abs_val)
    nano = int((abs_val - units) * 1_000_000_000 + Decimal("0.5"))
    # Гарантируем 0 <= nano < 1_000_000_000
    if nano >= 1_000_000_000:
        units += nano // 1_000_000_000
        nano = nano % 1_000_000_000
    return Quotation(units=sign * units, nano=sign * nano)


def _map_post_order_error(exc: RequestError) -> OrderError:
    """Маппинг RequestError (gRPC) → наше исключение."""
    # Специфичные статусы → конкретные классы
    if exc.code == grpc.StatusCode.NOT_FOUND:
        return OrderNotFoundError("unknown")
    if exc.code == grpc.StatusCode.INVALID_ARGUMENT:
        return OrderValidationError(str(exc.details))
    if exc.code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.FAILED_PRECONDITION):
        return OrderExchangeUnavailable(str(exc.details))

    # Fallback: по тексту сообщения
    details = (exc.details or "").lower()
    if "rejected" in details:
        return OrderRejected(str(exc.details), reject_reason=exc.details)
    if "not found" in details:
        return OrderNotFoundError("unknown")

    return OrderError(str(exc.details), code=str(exc.code))


# --- Основной класс ----------------------------------------------------------


class TinkoffOrders:
    """Реализация OrderGateway поверх TinkoffClient.

    Использование:
        client = TinkoffClient(cfg)
        with client:
            orders = TinkoffOrders(client)
            state = await orders.post_order(order)
    """

    def __init__(self, client: TinkoffClient) -> None:
        self._client = client
        self._clock = SystemClock()

    # --- Публичный API --------------------------------------------------------

    async def post_order(self, order: Order) -> OrderStateInfo:
        """Отправить заявку. Идемпотентность через order.order_request_id.

        Raises:
            OrderRejected: биржа/брокер отклонил заявку
            OrderValidationError: некорректные параметры
            OrderExchangeUnavailable: биржа недоступна
            OrderError: другая ошибка gRPC
        """
        sdk = self._client.sdk

        # Маппинг направления
        direction = _SIDE_TO_DIRECTION.get(order.side)
        if direction is None:
            raise OrderValidationError(f"Неподдерживаемое направление: {order.side}")

        # Маппинг типа заявки
        order_type_sdk = _ORDER_TYPE_MAP.get(order.order_type.value)
        if order_type_sdk is None:
            raise OrderValidationError(
                f"Неподдерживаемый тип заявки: {order.order_type}"
            )

        # Цена — только для лимитной заявки
        price = None
        if order.order_type.value == "limit" and order.price is not None:
            price = _decimal_to_quotation(order.price)

        try:
            resp = sdk.orders.post_order(
                figi=order.figi,
                quantity=order.quantity,
                price=price,
                direction=direction,
                account_id=order.account_id,
                order_type=order_type_sdk,
                order_id=order.order_request_id,
            )
        except RequestError as exc:
            raise _map_post_order_error(exc) from exc

        return self._map_post_order_response(resp, order.account_id)

    async def cancel_order(
        self, account_id: str, order_request_id: str
    ) -> OrderStateInfo:
        """Снять активную заявку.

        T-Invest API принимает `order_id` (биржевой ID заявки), а не
        `order_request_id` (наш UUID). Поэтому сначала получаем текущий
        статус заявки через `get_order_state`, чтобы извлечь `order_id`,
        затем отменяем.

        Альтернативно, если `order_request_id` совпадает с `order_id`
        (T-Invest может использовать наш order_request_id как order_id),
        пробуем отменить напрямую.
        """
        sdk = self._client.sdk

        # Сначала получаем order_id через get_order_state или get_orders
        order_id = await self._find_order_id(account_id, order_request_id)

        try:
            resp = sdk.orders.cancel_order(
                account_id=account_id,
                order_id=order_id,
            )
        except RequestError as exc:
            raise _map_post_order_error(exc) from exc

        # CancelOrderResponse содержит только time — берём из него
        ts = tinkoff_time(resp.time) if hasattr(resp, "time") and resp.time else self._clock.now()
        return OrderStateInfo(
            order_request_id=order_request_id,
            account_id=account_id,
            state=OrderState.CANCELLED,
            filled_quantity=0,
            average_price=None,
            reject_reason=None,
            timestamp=ts,
        )

    async def stream_order_states(self) -> AsyncIterator[OrderStateInfo]:
        """Polling статусов заявок + история операций для FILLED.

        В текущей версии — polling через:
        1. `get_orders` (список активных заявок) — для отслеживания NEW/PARTIALLYFILL/CANCELLED.
        2. `operations.get_operations` — для обнаружения уже исполненных (FILLED) заявок,
           которые исчезли из активного списка.

        При необходимости точной реалтайм-доставки заменить на server-streaming
        `orders_stream.trades_stream`.

        Эмитит события только при изменении состояния (сравниваем с предыдущим).
        Каждый poll-цикл обёрнут в asyncio.wait_for(timeout=15) чтобы избежать
        бесконечного зависания gRPC на медленных/упавших endpoint'ах.
        """
        sdk = self._client.sdk
        seen: dict[str, str] = {}  # order_id → state_str
        last_op_ts: Optional[datetime] = None

        while True:
            try:
                # Таймаут на один poll-цикл — 15 сек
                accounts_resp = await asyncio.wait_for(
                    self._get_accounts(sdk), timeout=15.0
                )
                for acct in accounts_resp.accounts:
                    # 1. Активные заявки
                    try:
                        orders_resp = await asyncio.wait_for(
                            self._get_orders(sdk, acct.id), timeout=15.0
                        )
                    except Exception:  # noqa: BLE001
                        continue
                    for order_state in orders_resp.orders:
                        state_str = execution_status_to_order_state(
                            order_state.execution_report_status
                        )
                        order_id = order_state.order_id
                        prev_state = seen.get(order_id)
                        if prev_state is None or prev_state != state_str:
                            seen[order_id] = state_str
                            yield self._map_order_state(
                                order_state, acct.id
                            )

                    # 2. Исполненные заявки через операции
                    async for state_info in self._poll_filled_operations(
                        sdk, acct.id, seen, last_op_ts
                    ):
                        order_id = state_info.order_request_id
                        seen[order_id] = state_info.state.value
                        if state_info.timestamp:
                            if last_op_ts is None or state_info.timestamp > last_op_ts:
                                last_op_ts = state_info.timestamp
                        yield state_info
            except asyncio.TimeoutError:
                log.warning("stream_order_states poll timeout")
            except Exception as exc:  # noqa: BLE001
                log.warning("stream_order_states poll error: %s", exc)

            await asyncio.sleep(2.0)

    @staticmethod
    async def _get_accounts(sdk) -> Any:
        """Обёртка для get_accounts() с возможностью таймаута."""
        return sdk.users.get_accounts()

    @staticmethod
    async def _get_orders(sdk, account_id: str) -> Any:
        """Обёртка для get_orders() с возможностью таймаута."""
        return sdk.orders.get_orders(account_id=account_id)

    async def _poll_filled_operations(
        self,
        sdk,
        account_id: str,
        seen: dict[str, str],
        since: Optional[datetime],
    ) -> AsyncIterator[OrderStateInfo]:
        """Получить FILLED-состояния из operations.get_operations.

        В песочнице исполненная заявка быстро исчезает из get_orders.
        Через get_operations можно достать биржевые операции и восстановить
        order_request_id из operation_id (T-Invest обычно копирует order_id).
        """
        from core.models import Side

        _OP_TYPE_TO_SIDE = {
            "OPERATION_TYPE_BUY": Side.BUY,
            "OPERATION_TYPE_BUY_CARD": Side.BUY,
            "OPERATION_TYPE_BUY_MARGIN": Side.BUY,
            "OPERATION_TYPE_DELIVERY_BUY": Side.BUY,
            "OPERATION_TYPE_SELL": Side.SELL,
            "OPERATION_TYPE_SELL_CARD": Side.SELL,
            "OPERATION_TYPE_SELL_MARGIN": Side.SELL,
            "OPERATION_TYPE_DELIVERY_SELL": Side.SELL,
        }

        try:
            from datetime import timedelta

            now = self._clock.now()
            from_ts = since or (now - timedelta(hours=24))
            ops_resp = await asyncio.wait_for(
                self._get_operations(sdk, account_id, from_ts, now),
                timeout=15.0,
            )
            log.info(
                "_poll_filled_operations: account=%s ops=%d since=%s",
                account_id, len(ops_resp.operations), from_ts.isoformat()
            )
            for op in ops_resp.operations:
                log.info(
                    "op: id=%s figi=%s state=%s payment=%s qty=%s type=%s",
                    op.id, op.figi, op.state,
                    op.payment, getattr(op, "quantity", None), op.type
                )
                if op.state != 1:  # OPERATION_STATE_EXECUTED
                    continue
                if op.figi is None:
                    continue
                order_id = op.id or ""
                if seen.get(order_id) == "filled":
                    continue
                if not order_id:
                    continue

                side = _OP_TYPE_TO_SIDE.get(getattr(op, "type", None))
                if side is None and hasattr(op, "operation_type"):
                    side = _OP_TYPE_TO_SIDE.get(op.operation_type.name)

                avg_price = None
                qty = int(getattr(op, "quantity", 0))
                if op.payment and qty:
                    avg_price = abs(quotation_to_decimal(op.payment)) / qty
                if avg_price is None and hasattr(op, "price") and op.price and qty:
                    avg_price = quotation_to_decimal(op.price)

                log.info(
                    "Emitting FILLED from operation: order_id=%s figi=%s side=%s qty=%s price=%s",
                    order_id, op.figi, side.value if side else None, qty, avg_price
                )
                yield OrderStateInfo(
                    order_request_id=order_id,
                    account_id=account_id,
                    state=OrderState.FILLED,
                    filled_quantity=qty,
                    average_price=avg_price,
                    reject_reason=None,
                    figi=op.figi,
                    side=side,
                    timestamp=tinkoff_time(op.date) if op.date else self._clock.now(),
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("_poll_filled_operations error: %s", exc)

    @staticmethod
    async def _get_operations(sdk, account_id: str, from_ts: datetime, to_ts: datetime) -> Any:
        """Обёртка для operations.get_operations() с возможностью таймаута."""
        return sdk.operations.get_operations(
            account_id=account_id,
            from_=from_ts,
            to=to_ts,
        )

    # --- Внутренние методы -----------------------------------------------------

    def _map_post_order_response(
        self, resp, account_id: str
    ) -> OrderStateInfo:
        """Маппинг PostOrderResponse → OrderStateInfo."""
        state_str = execution_status_to_order_state(
            resp.execution_report_status
        )
        state = OrderState(state_str)

        avg_price = None
        if hasattr(resp, "executed_order_price") and resp.executed_order_price:
            avg_price = quotation_to_decimal(resp.executed_order_price)

        reject_reason = None
        if state == OrderState.REJECTED:
            reject_reason = getattr(resp, "message", None)

        # T-Invest возвращает order_id (биржевой), но наш интерфейс
        # использует order_request_id. В PostOrderResponse order_id
        # обычно совпадает с нашим order_request_id, если мы его задали.
        ts = self._clock.now()
        return OrderStateInfo(
            order_request_id=getattr(resp, "order_id", ""),
            account_id=account_id,
            state=state,
            filled_quantity=int(getattr(resp, "lots_executed", 0)),
            average_price=avg_price,
            reject_reason=reject_reason,
            timestamp=ts,
        )

    def _map_order_state(self, state_obj, account_id: str) -> OrderStateInfo:
        """Маппинг OrderState (SDK) → core.OrderStateInfo."""
        from core.models import Side

        state_str = execution_status_to_order_state(
            state_obj.execution_report_status
        )

        avg_price = None
        if hasattr(state_obj, "average_position_price") and state_obj.average_position_price:
            avg_price = quotation_to_decimal(state_obj.average_position_price)

        reject_reason = None
        if state_str == "rejected":
            reject_reason = getattr(state_obj, "message", None)

        ts = self._clock.now()
        if hasattr(state_obj, "order_date") and state_obj.order_date:
            ts = tinkoff_time(state_obj.order_date)

        figi = getattr(state_obj, "figi", None) or None
        side: Optional[Side] = None
        direction = getattr(state_obj, "direction", None)
        if direction is not None:
            dir_val = direction.value if hasattr(direction, "value") else direction
            if dir_val == 1:
                side = Side.BUY
            elif dir_val == 2:
                side = Side.SELL

        return OrderStateInfo(
            order_request_id=getattr(state_obj, "order_request_id", "") or getattr(state_obj, "order_id", ""),
            account_id=account_id,
            state=OrderState(state_str),
            filled_quantity=int(getattr(state_obj, "lots_executed", 0)),
            average_price=avg_price,
            reject_reason=reject_reason,
            figi=figi,
            side=side,
            timestamp=ts,
        )

    async def _find_order_id(
        self, account_id: str, order_request_id: str
    ) -> str:
        """Найти биржевой order_id по нашему order_request_id.

        T-Invest может использовать order_request_id как order_id
        (при создании через post_order с order_id параметром).
        Сначала проверяем совпадение, затем ищем в списке заявок.
        """
        sdk = self._client.sdk

        # Пробуем get_order_state — если order_request_id = order_id
        try:
            state = sdk.orders.get_order_state(
                account_id=account_id,
                order_id=order_request_id,
            )
            return state.order_id
        except RequestError as exc:
            if exc.code != grpc.StatusCode.NOT_FOUND:
                raise

        # Ищем в списке всех активных заявок
        try:
            orders_resp = sdk.orders.get_orders(account_id=account_id)
            for o in orders_resp.orders:
                if getattr(o, "order_request_id", "") == order_request_id:
                    return o.order_id
        except RequestError:
            pass

        # Fallback: предполагаем, что order_request_id = order_id
        log.warning(
            "order_request_id=%s не найден в списке заявок, "
            "используем как order_id напрямую",
            order_request_id,
        )
        return order_request_id