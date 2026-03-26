# ТЗ: Core + Storage

## Зона ответственности

- общий parser contract
- схема `Event`
- SQLite schema и repository layer
- lifecycle запуска

## Что на входе

- один `.log` файл
- один выбранный профиль

## Что на выходе

- нормализованные `Event` в SQLite
- `manifest.json`
- `run_summary.json`
- `events.csv`

## Готово, когда

- lifecycle по `run_id` стабилен
- agent tools читают результат без повторного парсинга
- integration tests проходят для всех профилей
