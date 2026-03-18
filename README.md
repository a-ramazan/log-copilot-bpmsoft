# LogCopilot

LogCopilot превращает сырой лог в:

- `events.csv` — структурированные события
- `clusters.csv` — сигнатурные кластеры
- `semantic_clusters.csv` — semantic groups поверх сигнатур
- `analysis_summary.json` — coverage и quality metrics
- `trace_summary.json` — тайминги, counters, normalization audit
- `debug_samples.md` — примеры `raw -> normalized -> cluster`

## How To Run

1. Слить много файлов в один:

```bash
python scripts/merge_logs.py --input Logs/2025-10-20 --out data/merged.log
```

Merged file по умолчанию сохраняется в `data/merged.log`.

2. Запустить pipeline:

```bash
python -m logcopilot.pipeline --input data/merged.log --out out --clean-out --log-level DEBUG --sample-events 20
```

3. Открыть notebook:

```bash
jupyter notebook notebooks/EDA.ipynb
```

## Что делает pipeline

Архитектура всегда одна и та же:

`parsing -> normalization -> signature clustering -> semantic clustering -> reporting`

- `parsing.py` режет лог на события
- `normalization.py` убирает шум и считает mask stats
- `signatures.py` строит signature и embedding text
- `clustering.py` делает baseline clustering по `signature_hash`
- `semantic.py` объединяет похожие сигнатуры через embeddings
- `quality.py` считает coverage/confidence
- `reporting.py` пишет артефакты
- `pipeline.py` всё связывает и управляет run-level trace

## Semantic Layer

Semantic clustering включён по умолчанию.

Если зависимостей нет, pipeline должен завершиться с понятной ошибкой. Установить всё можно так:

```bash
pip install -r requirements.txt
```

По умолчанию используется модель:

`sentence-transformers/all-MiniLM-L6-v2`

## Notebook / EDA

Notebook `notebooks/EDA.ipynb` умеет:

- запускать pipeline как Python-функцию `run_pipeline(...)`
- загружать артефакты из одного `OUT_DIR`
- показывать summary metrics
- исследовать top clusters
- смотреть raw/normalized примеры
- смотреть normalization audit
- переигрывать semantic clustering с другими параметрами без повторного парсинга
