# Robot — Алгоритмическая торговля на ММВБ (MVP)

Автоматическая paper-трейдинг система для фьючерса Si на Московской бирже через брокера **Т-Инвестиции** (gRPC API) в sandbox-режиме.

## Что входит в MVP

- Стратегия **Donchian Breakout v2** (Turtle 20/10 + фильтр ADX>15) на Si/1h.
- Paper-trading runner, подключающийся к песочнице Т-Инвест.
- Риск-менеджер: kill-switch, лимит позиции, дневной убыток ≤ 5%.
- Журнал сделок (SQLite), PnL-калькулятор, ежедневный Markdown-отчёт.
- Heartbeat, watchdog, preflight чек-лист.
- Сверка paper-P&L с бэктестом.

## Архитектура

```
02-source/
├── core/              # модели pydantic, событийная шина, интерфейсы, часы
├── broker/tinvest/    # адаптер T-Invest gRPC
├── risk/              # RiskManager, KillSwitch
├── journal/           # SQLite-журнал, PnL, daily report
└── strategies/swing/donchian_breakout_v2/  # основная стратегия MVP

05-bots/               # runner + longrun + конфиг
03-scripts/            # эксплуатационные скрипты
```

Слои изолированы: `broker → risk → strategy → runner`.

## Быстрый старт

```bash
# Python 3.12 (3.14 не поддерживается из-за protobuf)
python --version  # 3.12.x

# Виртуальное окружение
python -m venv .venv
.venv\Scripts\activate  # Windows

# Зависимости
pip install -r requirements.txt

# Переменные окружения
cp .env.example .env
# Заполнить T_INVEST_SANDBOX_TOKEN (обязательно для paper)
```

## Запуск

```bash
# Preflight перед long-run
.venv\Scripts\python 03-scripts/preflight.py

# Long-run paper-trading (одна сессия)
.venv\Scripts\python 05-bots/longrun.py --one-session

# Watchdog (one-shot)
.venv\Scripts\python 03-scripts/watchdog.py --one-shot

# Сверка paper-P&L с бэктестом
.venv\Scripts\python 03-scripts/compare_backtest.py --date YYYY-MM-DD
```

## Конфигурация

Основной конфиг: `05-bots/donchian_paper.yaml`.

Ключевые параметры:
- `mode: paper` — только sandbox.
- `broker.ticker: Si` — фьючерс USD/RUB.
- `strategy.config` — Donchian v2 (entry_period=20, exit_period=10, adx_min=15).
- `risk.max_daily_loss_pct: 0.05` — авто kill-switch при −5%.

## Документация

- `CLAUDE.md` — правила работы с проектом.
- `01-docs/strategies/donchian_v2.md` — описание стратегии.
- `01-docs/runbook_longrun.md` — инструкция по эксплуатации long-run.

## Критерий перехода к live

1. Paper-трейдинг ≥ 4 недель.
2. Расхождение paper-P&L с бэктестом ≤ 30%.
3. Нет критических сбоев runner/journal/heartbeat.
4. Kill-switch и watchdog работают корректно.

## Лицензия

Проект ведётся для личного использования. Перед live-торговлей требуется ≥4 недели paper-trading.
