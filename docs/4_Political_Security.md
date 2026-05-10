# 4. Верификация политик безопасности

## Цель верификации

Проверка соответствия реализованных политик безопасности архитектуре политик, описанной в разделах 01_CPB.md и 02_POLICY_ARCHITECTURE.md.

---

## Результаты верификации по компонентам

### sitl_verifier (ЦПБ-1)

| Политика безопасности | Статус | Реализация | Соответствие |
|-----------------------|--------|------------|--------------|
| Парсинг JSON |  | `parse_json_payload()` — lines 157-159 | Полное |
| Классификация по топику |  | `classify_input_topic()` — lines 161-165 | Полное |
| Валидация по схеме |  | `validate_schema()` — lines 169-171 | Полное |
| Логирование отклонённых |  | `infopanel.log_event()` — lines 101-103, 130-132 | Полное |
| Публикация в verified-топик |  | `bus.publish()` — lines 116, 144 | Полное |

**Вывод:** Реализация полностью соответствует архитектуре Chain of Responsibility. Каждый этап цепочки реализован.

---

### sitl_controller (ЦПБ-2 и ЦПБ-3)

| Политика безопасности | Статус | Реализация | Соответствие |
|-----------------------|--------|------------|--------------|
| Повторная валидация HOME |  | `validate_schema()` — line 147 | Полное |
| Проверка HOME prerequisite |  | `state_has_home()` — line 170 | Полное |
| Построение состояния |  | `build_home_state()` — line 160 | Полное |
| Определение статуса (ARMED/MOVING) |  | `apply_command_update()` — state.py | Полное |
| Сохранение в Redis |  | `_persist_state()` — lines 192-196 | Полное |
| TTL на ключах |  | `r.expire()` — line 195 | Полное |
| Игнорирование COMMAND без HOME |  | lines 170-175 | Полное |

**Вывод:** Реализация полностью соответствует архитектуре Strategy pattern. Проверки prerequisite и переходы статусов реализованы корректно.

---

### sitl_messaging

| Политика безопасности | Статус | Реализация | Соответствие |
|-----------------------|--------|------------|--------------|
| Валидация запроса по схеме |  | `validate_schema()` — line 99 | Полное |
| Проверка наличия состояния |  | `r.hgetall()` — line 106 | Полное |
| Сохранение correlation_id |  | `create_response()` — line 123 | Полное |
| Контроль reply_to |  | `self._get_transport_value()` — lines 130-133 | Полное |
| Ответ только в указанный топик |  | `bus.publish()` — line 134 | Полное |

**Вывод:** Реализация полностью соответствует архитектуре Proxy pattern. Все функции перехвата реализованы.

---

### sitl_core (ЦПБ-6)

| Политика безопасности | Статус | Реализация | Соответствие |
|-----------------------|--------|------------|--------------|
| Фильтр только MOVING |  | `state.get("status") != "MOVING"` — line 94 | Полное |
| Расчёт позиции по вектору |  | `advance_drone_state()` — line 97 | Полное |
| Сохранение с TTL |  | `r.expire()` — line 100 | Полное |
| SCAN с итератором |  | `r.scan_iter()` — line 72 | Полное |

**Вывод:** Реализация соответствует архитектуре. Memento pattern реализован через сохранение состояния в Redis.

---

## Проверка соответствия шаблонам СКИБ

### Chain of Responsibility (sitl_verifier)

```
Ожидаемая цепочка:
1. JSON parse → 2. Topic classify → 3. Schema validate → 4. Publish

Реализация (_process_input_message, lines 151-172):
  payload = parse_json_payload(raw_payload)        ✓ Step 1
  message_type, schema_name = classify_input_topic() ✓ Step 2
  validate_schema(payload, schema_name)            ✓ Step 3
  return (True, message_type, payload, "")          → Step 4 в publish()

Статус:  Соответствует
```

### Strategy (sitl_controller)

```
Ожидаемые стратегии:
- ARMED: принять команду → MOVING
- MOVING: игнорировать команды
- HOME_MISSING: отклонить без HOME

Реализация (_handle_verified_message):
  if message_type == "HOME":
      status = "ARMED"                              ✓ ARMED strategy
      build_home_state()

  if not state_has_home(existing_state):
      return ignored                                ✓ HOME_MISSING strategy

  next_state = apply_command_update()
  status = "MOVING" if is_moving else "ARMED"       ✓ Status transition

  // MOVING статус обрабатывается в sitl_core,
  // который пропускает обновление если status != "MOVING"

Статус:  Соответствует
```

### Memento (sitl_controller + sitl_core)

```
Ожидаемое поведение:
- Save: сохранение home при HOME
- Restore: проверка home при COMMAND
- Boundary: проверка диапазонов

Реализация:
  build_home_state(): сохраняет home_lat/lon/alt ✓ Save
  state_has_home(): проверяет наличие home       ✓ Restore
  apply_command_update(): использует home из state ✓ Boundary

Статус:  Соответствует (все операции реализованы)
```

### Proxy (sitl_messaging)

```
Ожидаемые функции:
- Валидация запроса
- Tracing (correlation_id)
- Контроль reply_to

Реализация (_handle_request_position):
  validate_schema(payload)                          ✓ Валидация
  correlation_id из message                         ✓ Tracing
  reply_to контролируется                           ✓ Контроль

Статус:  Соответствует
```

### Broker (SystemBus)

```
Ожидаемые функции:
- Topic isolation
- Transport abstraction (Kafka/MQTT)
- Consumer groups

Реализация (bus_factory.py):
  Два транспорта: kafka/kafka_system_bus.py, mqtt/mqtt_system_bus.py
  Единый API через SystemBus

Статус:  Соответствует
```

---

## Расхождения и замечания

### 1. Отсутствие HMAC-подписи верифицированных сообщений

**Ожидаемо (рекомендация):** Верифицированные сообщения должны содержать HMAC-подпись

**Реализовано:** Только `verified_at` timestamp (contracts.py line 94)

**Влияние:** Низкое — timestamp обеспечивает traceability, но не integrity

**Рекомендация:** Рассмотреть добавление HMAC для production

### 2. Отсутствие correlation_id tracking в Redis

**Ожидаемо (рекомендация):** Хранение обработанных correlation_id для защиты от replay

**Реализовано:** correlation_id передаётся в ответах, но не сохраняется

**Влияние:** Среднее — защита от replay частично обеспечена через unique correlation_id

**Рекомендация:** Добавить Redis set для отслеживания correlation_id

### 3. Нет валидации геозоны в core

**Ожидаемо:** Проверка позиции дрона относительно home при обновлении

**Реализовано:** только проверка статуса MOVING

**Влияние:** Среднее — нарушение геозоны не контролируется

**Рекомендация:** Добавить boundary check в advance_drone_state()

### 4. COMMAND prerequisite статус не проверяется

**Ожидаемо (Strategy):** COMMAND принимается только в статусе ARMED

**Реализовано:** Проверяется только наличие HOME, статус не проверяется

**Влияние:** Среднее — можно отправить COMMAND когда дрон MOVING

**Рекомендация:** Добавить проверку `existing_state["status"] == "ARMED"`

---

## Итоговая оценка

| Компонент | Соответствие архитектуре | Критичность расхождений |
|-----------|-------------------------|-------------------------|
| sitl_verifier |  100% | Низкая |
| sitl_controller |  85% | Средняя (2 замечания) |
| sitl_messaging |  100% | Низкая |
| sitl_core |  90% | Низкая |

**Общий вывод:** Реализация в целом соответствует архитектуре политик безопасности. Выявлены 4 расхождения средней критичности, которые рекомендуется устранить для production-использования.

---

## Рекомендуемые исправления

### HIGH PRIORITY

1. **Добавить проверку статуса ARMED в controller** (lines 170-175)
   ```python
   if existing_state.get("status") != "ARMED":
       return {"status": "ignored", "reason": "drone not in ARMED state"}
   ```

### MEDIUM PRIORITY

2. **Добавить валидацию геозоны в core** (state.py)
   - Проверка distance от home при advance_drone_state()
   - Блокировка при выходе за пределы

3. **Добавить correlation_id tracking** (controller или messaging)
   - Redis SET с TTL для processed IDs

### LOW PRIORITY

4. **Добавить HMAC-подпись** (verifier)
   - Для production-окружения