# `logcopilot/core`

## Назначение

Общее ядро обработки. Здесь сырой лог превращается в стабильный `Event`, который потом используют все сценарии.

## Что на входе

- один `.log` файл
- raw output из `logcopilot.parsing`

## Что на выходе

- нормализованные `Event` с общими полями: timestamp, level, ids, HTTP-метаданные, normalized message, incident flag

## Зависимости

- `logcopilot.parsing`
- `logcopilot.normalization`
- `logcopilot.signatures`

## Что не менять без причины

- поля `Event`
- логику нормализации
- названия parser profile

## Как проверять

```bash
python -m unittest tests.test_parsing tests.test_normalization
```
