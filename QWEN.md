## Qwen Added Memories
- SITL-module — статус на 12.04.2026:

**Исправлено:**
1. docker-compose.yaml — все контейнеры используют `pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt` вместо ненадёжной проверки import
2. Makefile — `make init` скачивает пакеты в pip-cache/; добавлен `timeout /t 15` перед тестами; все test-команды используют mirror
3. sitl_verifier.py — добавлены `_log_callback_error` через `add_done_callback` для видимости ошибок
4. sitl_controller.py — добавлены `_log_callback_error` + debug-логи в `_persist_state`
5. test_full_lifecycle.py — шаг 3 теперь ждёт 2с + polling для обновления позиции Core
6. schemas/sitl-position-request.json — добавлено поле `action`
7. mosquitto.conf — `log_type all` УБРАН (вернул к базовому)

**Все 4 интеграционных теста ПРОЙДЕНЫ через `make integration-test-mqtt`**

**Нерешённые проблемы:**
- `make integration-test-mqtt` проходит, НО ручная отправка команд через `mosquitto_pub` из контейнера mosquitto НЕ работает (пустые payload) — причина в `-n` флагах создавших retained empty messages. Python paho-mqtt работает корректно.
- Controller иногда падает с `[Errno -2] Name or service not known` — это происходит при пересоздании контейнеров (--build), когда DNS ещё не готов. Решается перезапуском.
- `test_cmd.py` отправляет COMMAND но Verifier не получает — возможно retained пустые сообщения от mosquitto_pub -r -n блокируют delivery
- Нужно: полностью очистить retained сообщения и протестировать COMMAND + REQUEST/RESPONSE через консоль

**Файлы для отладки (временные):**
- test_mqtt.py, test_mqtt2.py, test_cmd.py, test_redis.py, test_redis.py в корне проекта — можно удалить
- SITL-module — статус на 13.04.2026:

**Исправлено и работает:**
1. docker-compose.yaml — все контейнеры используют `pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt`
2. Makefile — добавлен `make init`; добавлен `timeout /t 15` перед тестами; все test-команды используют mirror
3. sitl_verifier.py — добавлены `_log_callback_error` через `add_done_callback`
4. sitl_controller.py — добавлены `_log_callback_error` + debug-логи убраны из `_persist_state`
5. test_full_lifecycle.py — шаг 3 ждёт 2с + polling для обновления позиции Core
6. schemas/sitl-position-request.json — добавлено поле `action`
7. mosquitto.conf — базовый конфиг без `log_type all`
8. .gitignore — добавлен pip-cache/

**Все 4 интеграционных теста ПРОЙДЕНЫ через `make integration-test-mqtt`**

**Ручная отправка через консоль РАБОТАЕТ:**
- MQTT топики используют `/` вместо `.`: `sitl/commands`, `sitl/verified-home`, `sitl/telemetry/request`, `sitl/telemetry/response`
- `sitl-drone-home` без изменений (нет точек)
- Python paho-mqtt работает корректно
- `mosquitto_pub` из контейнера НЕ работает (пустые payload) — проблема с `-n` флагом создающим retained empty messages

**Важно:** интеграционные тесты используют `MQTTSystemBus` который правильно конвертирует `.` → `/`. Ручные тесты через paho должны использовать `/` в топиках.

**Временные файлы удалены:** test_mqtt.py, test_mqtt2.py, test_cmd.py, test_redis.py
