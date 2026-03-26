# ТЗ: Incidents

## Зона ответственности

- signature clustering
- semantic grouping
- top incidents
- LLM-ready payload для агента

## Что на входе

- `List[Event]`

## Что на выходе

- `clusters.csv`
- `semantic_clusters.csv`
- `top_incidents.md`
- `llm_ready_clusters.json`

## Готово, когда

- старое incident-поведение не сломано
- regression tests проходят
- top incidents доступны через storage и agent tools
