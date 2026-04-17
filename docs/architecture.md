# Архитектура

## Поток выполнения

`input .log -> ingest -> parse -> normalize -> сохранить events -> посчитать профиль -> сохранить агрегаты -> записать артефакты -> agent читает storage -> при необходимости строит chat-level PNG visuals`

## Границы слоёв

- `core`: делает общий `Event` из сырого лога
- `profiles`: считает только свою аналитику
- `storage`: хранит run-ы, events, артефакты и агрегаты
- `agent`: работает только с готовыми результатами

## Кто за что отвечает

- `logcopilot/core`: парсерный контракт и общий event flow
- `logcopilot/storage`: схема SQLite и методы чтения/записи
- `logcopilot/profiles/incidents.py`: incident pipeline
- `logcopilot/profiles/heatmap.py`: heatmap pipeline
- `logcopilot/profiles/traffic.py`: traffic pipeline
- `logcopilot/agent`: tools и агентная оркестрация

## Как выглядит результат

```text
out/
  logcopilot.sqlite
  runs/
    <run_id>/
      manifest.json
      run_summary.json
      events.csv
      ...
```

## Совместимость

Старый запуск через `python -m logcopilot.pipeline` оставлен как thin wrapper над новым сценарием `incidents`.
