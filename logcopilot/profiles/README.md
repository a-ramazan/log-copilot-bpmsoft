# `logcopilot/profiles`

## Назначение

Здесь лежат профильные пайплайны. Каждый профиль получает один и тот же `Event` stream и строит свои артефакты и агрегаты.

## Что на входе

- `List[Event]`
- папка для сохранения артефактов

## Что на выходе

- `heatmap`: временные бакеты и hotspots
- `incidents`: кластеры, semantic groups, LLM-ready payload
- `traffic`: endpoint-агрегации и anomaly report

## Зависимости

- `logcopilot.core`
- `logcopilot.reporting`
- `logcopilot.storage`

## Что не менять без причины

- названия артефактов
- ключи summary
- соответствие профиля и SQLite-таблиц

## Как проверять

```bash
python -m unittest tests.test_profiles
```
