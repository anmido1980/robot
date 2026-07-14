---
name: robot-mvp
description: Работа с репозиторием Robot MVP — алгоритмическая торговля фьючерсом Si на ММВБ через Т-Инвест sandbox.
metadata:
  type: skill
  scope: project:robot
  repo_name: robot
---

# Skill: robot-mvp

Применяется при работе с репозиторием `Robot` — MVP алгоритмического трейдинга фьючерсом Si/1h на ММВБ через Т-Инвест gRPC (sandbox).

## When to use

1. Пользователь просит что-то сделать в папке `Robot/`.
2. Нужно запустить paper-trading runner, longrun, watchdog, preflight.
3. Нужно внести изменения в код/документацию/README.
4. Нужно проверить ошибки, heartbeat, журнал сделок.
5. Нужно подготовить проект к публикации/защите.

## How to work with this repository

### Основные правила

1. **Сначала показать план**, затем выполнять по пунктам после подтверждения пользователя.
2. **Результат каждого пункта сохранять в файл**.
3. **Не пушить на GitHub без явного указания пользователя**.
4. Действия с файлами внутри `Robot/` — без дополнительного подтверждения, кроме удаления.
5. Перед удалением файла в `04-output/` или `05-bots/` — сохранять `.bak`.
6. Все timestamps — UTC+3 (МСК), внутреннее хранение ISO 8601.
7. Python 3.12+; не использовать 3.14 из-за protobuf.
8. Запускать pytest через `.venv/Scripts/python -m pytest`.

### Структура репозитория (MVP)

```
Robot/
├── CLAUDE.md              # правила работы с проектом
├── README.md              # навигация
├── requirements.txt       # зависимости
├── .env.example           # шаблон переменных окружения
│
├── 01-docs/               # документация
│   ├── runbook_longrun.md
│   └── strategies/donchian_v2.md
├── 02-source/             # код
│   ├── core/              # модели pydantic, события, интерфейсы, часы, алерты
│   ├── broker/tinvest/    # адаптер T-Invest gRPC
│   ├── risk/              # RiskManager, KillSwitch, RiskConfig
│   ├── journal/           # SQLite-журнал, PnL, daily report
│   └── strategies/swing/donchian_breakout_v2/  # основная стратегия MVP
├── 03-scripts/            # эксплуатационные скрипты
│   ├── preflight.py
│   ├── watchdog.py
│   ├── compare_backtest.py
│   ├── rotate_logs.py
│   └── export_backtest_equity.py
├── 05-bots/               # runner + longrun + конфиг
│   ├── runner.py
│   ├── longrun.py
│   └── donchian_paper.yaml
└── .claude/skills/robot-mvp/  # этот skill
```

### Стек

- Python 3.12+
- `tinkoff-investments`
- pandas, numpy, pydantic, loguru, python-dotenv, pytest
- pyarrow (Parquet), pyyaml, aiohttp (Telegram)

### Основные команды

```bash
# Preflight перед long-run
.venv\Scripts\python 03-scripts/preflight.py

# Smoke paper-trading (10 минут, sandbox)
.venv\Scripts\python 03-scripts/smoke_runner.py --minutes 10

# Long-run (одна сессия)
.venv\Scripts\python 05-bots/longrun.py --one-session

# Watchdog one-shot
.venv\Scripts\python 03-scripts/watchdog.py --one-shot

# Export backtest equity CSV
.venv\Scripts\python 03-scripts/export_backtest_equity.py

# Compare paper-P&L with backtest
.venv\Scripts\python 03-scripts/compare_backtest.py --date YYYY-MM-DD
```

### Ключевые файлы кода

| Компонент | Файл |
|---|---|
| Runner | `05-bots/runner.py` |
| LongRun wrapper | `05-bots/longrun.py` |
| Конфиг paper | `05-bots/donchian_paper.yaml` |
| RiskManager | `02-source/risk/manager.py` |
| KillSwitch | `02-source/risk/kill_switch.py` |
| TradeJournal | `02-source/journal/trades.py` |
| PnL | `02-source/journal/pnl.py` |
| DailyReport | `02-source/journal/report.py` |
| Стратегия | `02-source/strategies/swing/donchian_breakout_v2/strategy.py` |
| T-Invest orders | `02-source/broker/tinvest/orders.py` |
| T-Invest market data | `02-source/broker/tinvest/market_data.py` |
| T-Invest portfolio | `02-source/broker/tinvest/portfolio.py` |
| Alerts | `02-source/core/alerts.py` |
| Models | `02-source/core/models.py` |

### Бизнес-ограничения

- Размер позиции ≤ 50% депозита.
- Дневной убыток ≤ 5% портфеля; kill-switch при −5%.
- Торговля только ММВБ фьючерсами (`class_code=SPBFUT`).
- Live только после ≥ 4 недель paper-trading.
- Без автоматической торговли в клиринг 23:50–00:30 МСК.

### Частые задачи

| Задача | Файл результата |
|---|---|
| План работ | `04-output/YYYY-MM-DD_plan.md` |
| Отчёт по багу | `04-output/YYYY-MM-DD_bugfix_report.md` |
| Статус paper-run | `04-output/YYYY-MM-DD_HH-MM_status.md` |
| Архитектурное решение | `04-output/YYYY-MM-DD_adr.md` |
| Обновление README | `README.md` |
| Обновление состояния | `01-docs/STATE.md` + `CLAUDE.md` (локально, STATE.md не в Git) |

### Публикация на GitHub

**ВАЖНО:** не выполнять `git push` без явного указания пользователя.

Если пользователь разрешил:
1. Проверить `.gitignore` — не должно быть `.env`, `06-logs/`, `*.db`.
2. Проверить отсутствие секретов в коммитах.
3. `git add`, `git commit -m "..."` с `Co-Authored-By: Claude <noreply@anthropic.com>`.
4. `git push origin master`.

### Проверка ошибок

1. Смотреть последний runner-лог: `06-logs/runs/runner_YYYY-MM-DD_HH-MM-SS.log`.
2. Смотреть heartbeat: `06-logs/heartbeat.json`.
3. Смотреть журнал: `06-logs/journal/donchian_v2.sqlite`.
4. Смотреть daily report: `01-docs/journal/YYYY-MM-DD.md`.
5. Проверить процессы: `tasklist | findstr python`.
6. Проверить `STOP.flag`.
