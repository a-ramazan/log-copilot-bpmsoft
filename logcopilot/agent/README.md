# `logcopilot/agent`

## Что это

Простой агент поверх уже обработанного `run_id`.

Он не читает сырой лог.
Он читает только данные из SQLite и отвечает по выбранному сценарию:

- `incidents`
- `heatmap`
- `traffic`

## Структура

- `config.py` — загрузка `.env` и выбор модели: `local` или `yandex`
- `tools.py` — детерминированный роутинг вопросов и чтение данных из БД
- `session.py` — простая память диалога внутри одной сессии чата
- `graph.py` — детерминированный runtime: `resolve_run -> route_question -> fetch_facts -> build_visual -> update_session`
- `prompts.py` — системный prompt для LLM
- `visuals.py` — on-demand PNG-графики для CLI
- `agent.py` — запуск runtime и генерация ответа через LLM
- `chat.py` — CLI-чат

## Как работает runtime

1. `resolve_run`
   Читает `run_id` и определяет, какой у него профиль.

2. `route_question`
   Детерминированно выбирает действие по вопросу.
   Пример:
   - для `incidents` может выбрать только incident-функции
   - для `traffic` может выбрать только traffic-функции

3. `fetch_facts`
   Забирает нужные данные из SQLite.

4. `build_visual`
   Если вопросу реально помогает график, строит PNG в `out/runs/<run_id>/charts/`.

5. `update_session`
   Запоминает, какой инцидент был выбран последним.
   Это нужно для follow-up вопросов вроде `в чем проблема?` или `как исправить?`

После runtime LLM получает:
- вопрос пользователя
- выбранное действие
- компактные факты из БД
- описание доступного графика или причину, почему он не построен

И уже на их основе пишет понятный ответ.

## Почему так

Здесь LLM не выбирает tools сама.
Выбор функций делается кодом, чтобы поведение было предсказуемым.

LLM нужна для другого:
- объяснить, что произошло
- кратко пересказать данные
- предложить гипотезы и шаги проверки

Графики тоже выбираются кодом, а не LLM.
Так чат остается предсказуемым и не тащит лишний контекст в prompt.

## Настройка

Один раз заполни `.env`.

Главное:

- для локальной модели:
  - `LOCAL_LLM_MODEL`
  - `LOCAL_LLM_BASE_URL`
  - `LOCAL_LLM_API_KEY`

- для Yandex:
  - `YC_FOLDER_ID`
  - `YC_AI_API_KEY`

## Запуск

Локальная модель:

```bash
python3 -m logcopilot.agent.chat --run-id <run_id> --provider local
```

Yandex:

```bash
python3 -m logcopilot.agent.chat --run-id <run_id> --provider yandex
```

Debug trace:

```bash
python3 -m logcopilot.agent.chat --run-id <run_id> --provider yandex --debug
```

## Что важно помнить

- стрим включён всегда
- БД одна: `out/logcopilot.sqlite`
- графики в чате сохраняются как PNG-артефакты в `out/runs/<run_id>/charts/`
- если `run_id` относится к `incidents`, агент не пойдёт в `heatmap` и `traffic`
- если `run_id` относится к `traffic`, агент не пойдёт в incident-функции
