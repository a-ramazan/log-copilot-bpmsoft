# Workflow команды

## Основное правило

- Интеграционная ветка одна: `main`
- Отдельную `dev` ветку сейчас не используем
- Любая разработка идёт в коротких feature-ветках
- В `main` изменения попадают только через PR

## Имена веток

- `feature/<scope>` — новая функциональность
- `fix/<scope>` — исправления
- `docs/<scope>` — изменения в документации

Текущие рабочие ветки команды:

- `feature/bootstrap-structure`
- `feature/agent`
- `feature/incidents`
- `feature/heatmap-traffic`

## Кто в какой ветке работает

- Карим — `feature/agent`
- Артур — `feature/incidents`
- Рамазан — `feature/heatmap-traffic`

Пока `feature/bootstrap-structure` не влита в `main`, новые большие задачи лучше не начинать.

## Правильный цикл работы

1. Переключиться на `main`
2. Подтянуть свежий `main`
3. Создать свою ветку от `main`
4. Работать только в своей зоне ответственности
5. Закоммитить изменения небольшими понятными коммитами
6. Запушить ветку
7. Открыть PR в `main`

Команды:

```bash
git checkout main
git pull origin main
git checkout -b feature/<scope>
git add -A
git commit -m "..."
git push -u origin feature/<scope>
```

## Пока PR открыт

- периодически подтягивать `main`
- конфликты решать в своей feature-ветке
- не пушить напрямую в `main`

Если `main` обновился:

```bash
git checkout main
git pull origin main
git checkout feature/<scope>
git merge main
```

## Pull Request

- PR всегда: `feature/<scope> -> main`
- до merge нужен минимум 1 review
- CI должен быть зелёным
- если менялся контракт output, это нужно явно написать в PR

## Что включить в GitHub

В настройках репозитория нужно включить для `main`:

- Branch protection
- Require a pull request before merging
- Require at least 1 approval
- Require status checks to pass before merging
- Block force pushes
- Auto-delete head branches after merge

## Labels

Создать такие labels:

- `core`
- `incidents`
- `heatmap`
- `traffic`
- `agent`
- `docs`
- `infra`
