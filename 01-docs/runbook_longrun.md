# Runbook — long-run paper-trading Donchian v2

**Стратегия:** Si/1h Donchian Breakout v2 (S1 + F1_ADX15)  
**Среда:** T-Invest sandbox (paper)  
**Расписание:** 09:55–23:45 МСК, пн–пт, без праздников  
**Цель:** автономный прогон 4+ недели для сравнения paper-P&L с бэктестом.

---

## 1. Быстрый старт

### Перед запуском

Запустить preflight:

```powershell
.venv/Scripts/python 03-scripts/preflight.py
```

Отчёт: `04-output/YYYY-MM-DD_preflight.md`. Если `Overall: OK` — можно запускать.

### Запуск long-run

В PowerShell (не закрывать окно):

```powershell
cd D:\Yandex.Disk\Project\Claude\Robot
.venv/Scripts/python 05-bots/longrun.py --config 05-bots/donchian_paper.yaml
```

Режим одной сессии (для smoke-теста):

```powershell
.venv/Scripts/python 05-bots/longrun.py --config 05-bots/donchian_paper.yaml --one-session
```

### Запуск watchdog в отдельном окне

```powershell
cd D:\Yandex.Disk\Project\Claude\Robot
.venv/Scripts/python 03-scripts/watchdog.py
```

Режим `--one-shot` для cron/ручного запуска:

```powershell
.venv/Scripts/python 03-scripts/watchdog.py --one-shot
```

---

## 2. Остановка

### Корректная остановка

Создать файл `STOP.flag` в корне проекта:

```powershell
New-Item -Path STOP.flag -ItemType File
```

Runner завершит текущую свечу/сделку, сохранит `06-logs/state.json` и выйдет. Watchdog при следующей проверке увидит отсутствие heartbeat, но не будет рестартовать, если `STOP.flag` присутствует.

### Экстренная остановка

Ctrl-C в окне `longrun.py`. Состояние P&L сохранится в `06-logs/state.json` при graceful shutdown.

### Перед повторным запуском

Удалить `STOP.flag`:

```powershell
Remove-Item STOP.flag
```

---

## 3. Мониторинг

### Heartbeat

Файл обновляется каждые 60 сек:

```text
06-logs/heartbeat.json
```

Поля: `timestamp`, `status`, `last_candle_time`, `last_price`, `daily_pnl`, `total_pnl`, `unrealized_pnl`, `position_qty`, `cash`, `active_orders`.

Проверить вручную:

```powershell
Get-Content 06-logs/heartbeat.json | ConvertFrom-Json
```

### Watchdog

Watchdog читает heartbeat каждые 2 мин. При проблеме пишет в лог и пытается рестартнуть `longrun.py`.

Лог watchdog:

```text
06-logs/runs/watchdog_YYYY-MM-DD.log
```

### Ежедневные отчёты

Торговый отчёт:

```text
01-docs/journal/YYYY-MM-DD.md
```

Сверка с бэктестом:

```text
04-output/YYYY-MM-DD_backtest_compare.md
```

### Логи runner

```text
06-logs/runs/runner_YYYY-MM-DD.log
06-logs/runs/longrun_YYYY-MM-DD.log
```

### Состояние

```text
06-logs/state.json
```

Содержит `pnl_state` — позиции, daily/total realized P&L, equity curve. Используется для восстановления после сбоя.

---

## 4. Журнал сделок

SQLite:

```text
06-logs/journal/donchian_v2.sqlite
```

Просмотр:

```powershell
.venv/Scripts/python -c "import sqlite3; print(sqlite3.connect('06-logs/journal/donchian_v2.sqlite').execute('select count(*), sum(realized_pnl) from trades').fetchall())"
```

---

## 5. Действия при kill-switch

### Признаки

- В heartbeat: `status: killed`.
- Telegram-алерт: `KILL SWITCH activated`.
- В логе: `KillSwitchActivated`.

### Что делать

1. **Не перезапускать автоматически.** Longrun.py не будет рестартовать после kill-switch до следующего торгового дня.
2. Открыть `01-docs/journal/YYYY-MM-DD.md` и посмотреть последние сделки.
3. Проверить `06-logs/state.json` — там последние позиции и P&L.
4. Убедиться, что позиции закрыты (runner вызывает `close_all_positions()` при kill-switch).
5. Проанализировать причину:
   - Ручной `STOP.flag` — удалить его перед следующим запуском.
   - Авто kill-switch (−5% за день) — пересмотреть размер позиции или рыночные условия.
6. Перед следующим запуском убедиться, что `STOP.flag` удалён и preflight проходит.

---

## 6. Действия при divergence > 30%

### Признаки

- Отчёт `04-output/YYYY-MM-DD_backtest_compare.md`: статус 🚨 CRITICAL.
- Telegram-алерт: `Backtest divergence`.

### Что делать

1. **Остановить longrun.py** (`STOP.flag`).
2. Сравнить paper-отчёт и backtest equity curve:
   - Разница в количестве сделок?
   - Разница в ценах исполнения (slippage)?
   - Пропущенные сигналы из-за клиринга/выходных?
3. Проверить журнал сделок на дубли/пропуски.
4. Возможные причины:
   - Бэктест overfit → остановить long-run.
   - Брокер/песочница не исполняет заявки → проверить `OrderState` в SQLite.
   - Стратегия не получает свежие свечи → проверить heartbeat `last_candle_time`.
5. После локализации причины — либо фикс, либо признать стратегию негодной к live.

---

## 7. Действия при stale heartbeat

### Признаки

- Watchdog лог: `heartbeat stale > 300 sec`.
- Telegram-алерт: `Heartbeat stale`.
- Runner может быть завис или упал.

### Что делать

1. Посмотреть последние строки `06-logs/runs/longrun_YYYY-MM-DD.log`.
2. Если runner завис — watchdog попытается рестартовать (max 3 раза в час).
3. Если рестарт не помог:
   - Убить процесс вручную.
   - Удалить `STOP.flag`, если он есть.
   - Перезапустить `longrun.py`.
4. Проверить `06-logs/state.json` — P&L должен восстановиться.

---

## 8. Действия при crash loop

### Признаки

- Watchdog лог: `crash loop detected`.
- Telegram-алерт: `Crash loop`.
- Longrun падает при каждом старте.

### Что делать

1. **Остановить watchdog** (Ctrl-C), чтобы не тратить попытки рестарта.
2. Запустить `longrun.py --one-session` вручную и записать traceback.
3. Проверить:
   - `.env` и токены.
   - Доступность T-Invest sandbox.
   - Наличие `06-logs/candles/Si_swing_1h.parquet`.
   - `06-logs/state.json` — возможно, повреждён. Переименовать в `state.json.bak` и перезапустить.
4. После фикса убрать `STOP.flag` и запустить watchdog заново.

---

## 9. Сбор диагностики

При обращении за помощью приложить:

1. `06-logs/heartbeat.json` (последний).
2. `06-logs/state.json`.
3. `01-docs/journal/YYYY-MM-DD.md` за проблемный день.
4. `04-output/YYYY-MM-DD_backtest_compare.md`.
5. `06-logs/runs/longrun_YYYY-MM-DD.log` и `runner_YYYY-MM-DD.log`.
6. `06-logs/runs/watchdog_YYYY-MM-DD.log`.
7. `06-logs/journal/donchian_v2.sqlite` (если вопрос по сделкам).

Архивировать:

```powershell
Compress-Archive -Path "06-logs/runs/*_2026-07-06.log", "06-logs/heartbeat.json", "06-logs/state.json" -DestinationPath "04-output/YYYY-MM-DD_diags.zip"
```

---

## 10. Плановое обслуживание

### Ротация логов и БД

Раз в неделю:

```powershell
.venv/Scripts/python 03-scripts/rotate_logs.py
```

### Сверка с бэктестом

Раз в день (можно поставить в планировщик задач Windows):

```powershell
.venv/Scripts/python 03-scripts/compare_backtest.py --date (Get-Date -Format "yyyy-MM-dd")
```

### Preflight перед каждым перезапуском

```powershell
.venv/Scripts/python 03-scripts/preflight.py
```

---

## 11. Контрольный список перед live

- [ ] 4+ недели paper-trading без критических сбоев.
- [ ] Divergence paper vs backtest ≤ 30% cumulative.
- [ ] Kill-switch сработал корректно при тесте (ручной и авто).
- [ ] Watchdog корректно рестартует runner после stale heartbeat.
- [ ] `.env` содержит `T_INVEST_PROD_TOKEN` и `TRADING_MODE=live`.
- [ ] Размер позиции в live = 0.5 от целевого на первые 2 недели.

---

## 12. Контакты и токены

- `.env` в корне проекта. Никогда не коммитить.
- Telegram-алерты: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
- Токены T-Invest: `T_INVEST_SANDBOX_TOKEN`, `T_INVEST_PROD_TOKEN`.

---

*Последнее обновление: 2026-07-06*
