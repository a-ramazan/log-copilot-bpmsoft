# `logcopilot/agent`

## Назначение

Слой для ИИ-агента. Tools читают только SQLite и готовые артефакты. Агент не должен сам разбирать сырые логи.

## Что на входе

- `run_id`
- при необходимости `db_path`
- стабильные методы storage

## Что на выходе

- summary по запуску
- поиск incident cluster
- heatmap данные
- traffic summary и anomalies
- содержимое артефактов

## Зависимости

- `logcopilot.storage`
- optional `langchain` / `langgraph`

## Что не менять без причины

- названия tools
- аргументы tools
- контракт чтения из storage

## Как проверять

```bash
python -m unittest tests.test_agent_tools
```

## Быстрый запуск первой версии

Один вопрос:

```bash
python -m logcopilot.agent.chat --run-id <run_id> --question "Покажи топ инциденты"
```

Интерактивный режим:

```bash
python -m logcopilot.agent.chat --run-id <run_id>
```

Если нужно включить старый rule-based режим без LLM:

```bash
python -m logcopilot.agent.chat --run-id <run_id> --mode rule
```
