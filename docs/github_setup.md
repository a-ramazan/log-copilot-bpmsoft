# Настройка GitHub

## 1. Защита ветки `main`

Зайти:

`GitHub -> Settings -> Branches -> Add rule`

Для `main` включить:

- Require a pull request before merging
- Require approvals: `1`
- Require status checks to pass before merging
- Do not allow force pushes
- Do not allow deletions

## 2. Автоудаление веток

Зайти:

`GitHub -> Settings -> General`

Включить:

- Automatically delete head branches

## 3. Labels

Зайти:

`GitHub -> Issues -> Labels`

Создать:

- `core`
- `incidents`
- `heatmap`
- `traffic`
- `agent`
- `docs`
- `infra`

## 4. PR процесс

- никто не пушит в `main` напрямую
- каждый работает в своей ветке
- merge только через PR
- после merge ветка удаляется
