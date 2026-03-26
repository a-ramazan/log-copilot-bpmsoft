# `logcopilot/storage`

## Назначение

SQLite-first слой хранения для runs, events, артефактов и профильных агрегатов.

## Что на входе

- `run_id`
- метаданные запуска
- `Event`
- агрегаты профилей

## Что на выходе

- данные в `out/logcopilot.sqlite`
- методы чтения для agent tools и проверок

## Зависимости

- стандартный `sqlite3`
- общие модели и reporting helpers

## Что не менять без причины

- имена таблиц
- ключи и уникальность
- формы ответа у query-методов

## Как проверять

```bash
python -m unittest tests.test_storage
```
