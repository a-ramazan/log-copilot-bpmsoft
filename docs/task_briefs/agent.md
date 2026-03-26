# ТЗ: Agent

## Зона ответственности

- tool layer поверх SQLite и артефактов
- LangGraph orchestration
- стабильный контракт prompt/tool

## Что на входе

- `run_id`
- стабильная схема storage

## Что на выходе

- tools для `incidents`, `heatmap`, `traffic`
- точка входа для графа агента

## Готово, когда

- агент не читает сырой лог напрямую
- tools возвращают предсказуемые структуры
- при отсутствии optional dependencies ошибка понятная
