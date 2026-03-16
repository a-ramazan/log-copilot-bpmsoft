# LogCopilot MVP

MVP-движок для BPMSoft логов: ingestion -> parsing -> normalization -> signature clustering -> top incidents report.

## Что умеет

- Рекурсивно читает все `.log` в папке.
- Склеивает multiline stacktrace в одно событие.
- Извлекает `timestamp`, `level`, `request_id`, `trace_id` там, где это возможно.
- Нормализует UUID, IP, email, токены, большие числа и даты.
- Строит сигнатуры ошибок по `normalized_message + exception_type + top stack frames`.
- Генерирует:
  - `out/events.csv`
  - `out/clusters.csv`
  - `out/top_clusters.md`
  - `out/events.parquet` при наличии `pyarrow`
  - `out/semantic_clusters.csv` при наличии ML-зависимостей и включённом semantic mode

## Быстрый запуск

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
python3 -m logcopilot.pipeline --input Logs --out out --semantic auto
```

`sentence-transformers/all-MiniLM-L6-v2` нужен только для semantic clustering. Для baseline clustering по сигнатурам скачивать модель не нужно.

