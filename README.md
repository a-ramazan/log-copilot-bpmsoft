# LogCopilot MVP

Explainable engine для логов: single-file intake -> parsing -> normalization -> embedding-ready events -> signature clustering -> quality metrics -> LLM-ready cluster payloads.

## Что умеет

- Принимает один лог-файл или директорию с `.log` файлами.
- Определяет parser profile: `log4net_like`, `plain_text`, `iis_w3c`, `generic_text`.
- Склеивает multiline stacktrace в одно событие.
- Сохраняет и `structured event`, и `raw_text`, чтобы не терять evidence.
- Извлекает `timestamp`, `level`, `component`, `request_id`, `trace_id`, `http_status`, если это возможно.
- Нормализует UUID, IP, email, токены, большие числа и даты.
- Строит `embedding_text` для semantic-слоя с fallback на raw event text.
- Строит сигнатуры ошибок по `normalized_message + exception_type + top stack frames`.
- Считает quality metrics и confidence:
  - parser coverage
  - fallback rate
  - cluster confidence
- Генерирует:
  - `out/events.csv`
  - `out/clusters.csv`
  - `out/analysis_summary.json`
  - `out/llm_ready_clusters.json`
  - `out/top_clusters.md`
  - `out/events.parquet` при наличии `pyarrow`
  - `out/semantic_clusters.csv` при наличии ML-зависимостей и включённом semantic mode

## Быстрый запуск

Один файл:

```bash
python3 -m logcopilot.pipeline --input Logs/2026-03-11/aspnetcore.log --out out
```

Папка логов:

```bash
python3 -m logcopilot.pipeline --input Logs --out out
```

## Опциональные зависимости

Базовый MVP работает без дополнительных библиотек.

Для parquet:

```bash
python3 -m pip install .[parquet]
```

Для semantic clustering:

```bash
python3 -m pip install .[semantic]
python3 -m logcopilot.pipeline --input Logs --out out
```

Semantic layer включён по умолчанию. Если зависимости или модель недоступны, pipeline не падает, а печатает причину пропуска semantic clustering.

`sentence-transformers/all-MiniLM-L6-v2` нужен только для semantic clustering. Для baseline clustering по сигнатурам скачивать модель не нужно.

## Как использовать с LLM

`out/llm_ready_clusters.json` — это готовый вход для следующего слоя, который пишет:

- title
- summary
- probable cause
- confidence
- evidence
- recommended checks

LLM должен работать поверх кластеров, а не поверх всего сырого файла.
