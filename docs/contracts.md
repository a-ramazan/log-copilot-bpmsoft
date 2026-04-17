# Контракты

## Контракт входа

- Один запуск принимает один `.log` файл.
- При запуске обязательно указывается один профиль:
  - `heatmap`
  - `incidents`
  - `traffic`

## Общий контракт события `Event`

Каждое нормализованное событие должно содержать:

- `event_id`
- `run_id`
- `source_file`
- `parser_profile`
- `timestamp`
- `level`
- `component`
- `message`
- `stacktrace`
- `raw_text`
- `line_count`
- `normalized_message`
- `signature_hash`
- `request_id`
- `trace_id`
- `http_status`
- `method`
- `path`
- `latency_ms`
- `response_size`
- `client_ip`
- `user_agent`
- `is_incident`

## Контракт запуска `run`

Для каждого запуска должны сохраняться:

- `run_id`
- `input_path`
- `profile`
- `created_at`
- `status`
- `event_count`
- `summary_json`

## Контракт артефактов

Общие артефакты:

- `manifest.json`
- `run_summary.json`
- `events.csv`
- `events.parquet`, если доступен parquet
- `charts/*.png`, если агент построил визуализацию в чате

Артефакты `heatmap`:

- `heatmap_timeseries.csv`
- `top_hotspots.md`

Артефакты `incidents`:

- `clusters.csv`
- `semantic_clusters.csv`
- `top_incidents.md`
- `llm_ready_clusters.json`

Артефакты `traffic`:

- `traffic_summary.csv`
- `latency_report.md`
- `suspicious_traffic.md`

## Контракт agent tools

- `get_run_summary(run_id, db_path="out/logcopilot.sqlite")`
- `get_top_incidents(run_id, limit=10, db_path="out/logcopilot.sqlite")`
- `find_incident_cluster(run_id, cluster_id, db_path="out/logcopilot.sqlite")`
- `get_heatmap(run_id, limit=50, db_path="out/logcopilot.sqlite")`
- `get_traffic_summary(run_id, status=None, limit=50, db_path="out/logcopilot.sqlite")`
- `get_traffic_anomalies(run_id, limit=20, db_path="out/logcopilot.sqlite")`
- `open_artifact(run_id, artifact_name, db_path="out/logcopilot.sqlite")`
