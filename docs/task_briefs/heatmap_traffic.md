# ТЗ: Heatmap + Traffic

## Зона ответственности

- hotspots по нагрузке и активности
- агрегации по endpoint/status/ip
- поиск подозрительного трафика

## Что на входе

- `List[Event]`

## Что на выходе

- `heatmap_timeseries.csv`
- `top_hotspots.md`
- `traffic_summary.csv`
- `latency_report.md`
- `suspicious_traffic.md`

## Готово, когда

- результаты пишутся и в файлы, и в SQLite
- тесты покрывают p95 и аномалии
- agent tools читают оба сценария без работы с сырым логом
