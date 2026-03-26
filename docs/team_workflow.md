# Workflow команды

## Ветки

- Работаем по trunk-based модели.
- Основная ветка: `main`.
- Рабочие ветки:
  - `feature/<scope>`
  - `fix/<scope>`
  - `docs/<scope>`
- Одна ветка = одна задача или одна зона изменений.

## Pull Request

- Любое изменение идёт через PR.
- До merge нужен минимум 1 approval.
- Тесты должны быть зелёными.
- В PR нужно писать, какие артефакты ожидаются после запуска.

## Labels

Создать в GitHub:

- `core`
- `incidents`
- `heatmap`
- `traffic`
- `agent`
- `docs`
- `infra`

## Definition of Done

- контракт обновлён в `docs/contracts.md`, если он менялся
- нужные тесты проходят
- README в затронутой зоне актуален
- выходные артефакты не нарушают договорённость

## Ownership

Сейчас в `CODEOWNERS` стоит `@Karimkhab` как базовый владелец. Потом можно заменить на реальные GitHub-аккаунты всей команды.

## Что включить в GitHub

- защита ветки `main`
- минимум 1 approval
- обязательные status checks
- запрет прямого push в `main`
